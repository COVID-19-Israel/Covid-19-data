import argparse
from datetime import datetime, timedelta
import csv
import os
import math

PROGRAM_DESCRIPTION = 'converts diff tables to a united states table.'
INPUT_HELP = 'path of csv diff tables dir (input)'
AREAS_HELP = 'path of csv explored areas table (input)'
OUTPUT_HELP = 'path of csv states table (output)'

WARNING_MISSING_COUNTRY = 'Warning: Country: "{0}" was not found in the explored areas file. Please add it, escpecially if it has provinces.'
WARNING_MISSING_PROVINCE = 'Warning: Province: "{0}" in Country: "{1}" was not found in the explored areas file. Please add it and other possible missing records.'
ERROR_MISSING_COUNTRY = 'Error: Country: "{0}" was not found.'
ERROR_MISSING_PROVINCE = 'Error: Province: "{0}" in Country: "{1}" was not found.'

ALL_PROVINCES = 'all'
START_DATE = '01/01/2019'
DATE_FORMAT = '%d/%m/%Y'

CONGREGARTION_LOCKDOWN_THRESHOLD = 100

# adds 01/01/2019 rows for each existing province and country
ADD_INITIAL_STATE = True

# in test mode, we don't do a couple of data modifications:
# 1. removing seconds from date - to help us track the order of rows within a certain day, we added incrementing seconds to the dates.
# 2. removing duplicate rows - sometimes multiple changes occur within a certain day, in which case we merge them to one row. this keeps also the old changes from a specific day.
# 3. fixing data - to mark the default state of 'congregation_restriction' we use math.inf. when displaying the data we convert it to None. Similar to 'distance_saving_instructions' (but with 0 and None).
TEST_MODE = False

# indices in diff tables
COUNTRY_INDEX = 0
PROVINCE_INDEX = 1
CHANGE_DATE_INDEX = 2
CHANGED_FIELD_INDEX = 3
PREV_FIELD_INDEX = 4
NEW_VALUE_INDEX = 5

STRINGENT_COUNTRY = 1
EASYGOING_COUNTRY = -1
IDENTICAL_DIRECTIVES = 0

VALID_TF_INPUTS = ['TRUE', 'FALSE']
VALID_LEVELS_INPUTS = ['0', '1', '2', 'NONE']

# true / false fields
TF_FIELDS = [
	'exposure_to_patient_isolation',
	'enter_area_isolation',
	'appearance_of_symptoms_isolation',
	'risk_groups_isolation',
	'full_quarantine',
	'partial_quarantine',
	'prohibition_entering_country',
	'limit_hospital_visits',
	'limit_retirement_homes_visits',
	'disinfecting_public_spaces',
	'police_army_enforcement',
	'fines_and_incarceration',
	'encouragement_using_masks',
	'encouragement_using_gloves',
	'encouragement_using_hand_sanitizers',
	'wide_temperature_tests'
]

# fields that are meant to be INFINITY in the normal state (without restrictions)
MAX_NORMAL_FIELDS = ['congregation_restriction']

# fields that are meant to be ZERO in the normal state (without restrictions)
MIN_NORMAL_FIELDS = ['distance_saving_instructions']

# levels fields refer to fields in which there are multiple severity levels, which are represented by incrementing numbers.
LEVELS_FIELDS = [
	'unnecessary_business',
	'educational_institutions',
	'religious_institutions',
	'public_transport_restriction'
]

# these fields should not be taken into account when calculating lockdown_level
LOCKDOWN_EXCLUSIONS = [
	'police_army_enforcement',
	'fines_and_incarceration'
]

DEFAULT_STATE = {
	'exposure_to_patient_isolation': False,
	'enter_area_isolation': False,
	'appearance_of_symptoms_isolation': False,
	'risk_groups_isolation': False,
	'full_quarantine': False,
	'partial_quarantine': False,
	'unnecessary_business': 0,
	'educational_institutions': 0,
	'religious_institutions': 0,
	'distance_saving_instructions': 0,
	'congregation_restriction': math.inf,
	'prohibition_entering_country': False,
	'public_transport_restriction': 0,
	'limit_hospital_visits': False,
	'limit_retirement_homes_visits': False,
	'disinfecting_public_spaces': False,
	'police_army_enforcement': False,
	'fines_and_incarceration': False,
	'encouragement_using_masks': False,
	'encouragement_using_gloves': False,
	'encouragement_using_hand_sanitizers': False,
	'wide_temperature_tests': False,
	'lockdown_level': 0
}

# a tool to know which provinces are under each country.
# 	data comes from an external csv file.
provinces_in_countries = {}

# the table this code creates. meant to hold all the states
# of the lockdown directives given their changes over time.
all_states_table = []


def create_initial_state(country_name, province_name):
	'''
	@purpose: creates the initial state row for a specific country or province
	'''

	initial_country_row = dict(DEFAULT_STATE)
	initial_country_row['country'] = country_name
	initial_country_row['province'] = province_name
	initial_country_row['start_date'] = datetime.strptime(START_DATE, DATE_FORMAT)

	all_states_table.append(initial_country_row)

	return initial_country_row

def is_state_restricted(state_row):
	'''
	@purpose: checks whether a state has ANY restrictions
	'''

	for field, value in state_row.items():
		if (field not in LOCKDOWN_EXCLUSIONS
			and (field in TF_FIELDS
				or field in MAX_NORMAL_FIELDS
				or field in MIN_NORMAL_FIELDS
				or field in LEVELS_FIELDS)):
			
			if DEFAULT_STATE[field] != value:
				return True

	return False


def calc_lockdown_level(state_row):
	'''
	@purpose: calculates the lockdown level (severity) for a specific state
	'''

	lockdown_level = 0

	if state_row['full_quarantine']:
		lockdown_level = 5

	elif state_row['partial_quarantine']:
		lockdown_level = 4

	elif state_row['unnecessary_business'] != 0:
		lockdown_level = 3

	elif (state_row['educational_institutions'] != 0
		or state_row['religious_institutions'] != 0
		or (state_row['congregation_restriction']
			and state_row['congregation_restriction'] <= CONGREGARTION_LOCKDOWN_THRESHOLD)

		or state_row['prohibition_entering_country']):
		lockdown_level = 2

	elif is_state_restricted(state_row):
		lockdown_level = 1

	return lockdown_level


def apply_diff(new_state_row, new_diff_row):
	'''
	@purpose: applies the value change from the diff table,
		and	adds the new row to the states table
	'''

	temp_state_row = dict(new_state_row)

	new_value = new_diff_row[NEW_VALUE_INDEX]
	field_name = new_diff_row[CHANGED_FIELD_INDEX]

	if field_name in TF_FIELDS:
		if new_value == 'TRUE':
			temp_state_row[field_name] = True

		elif new_value == 'FALSE':
			temp_state_row[field_name] = False

		else:
			raise ValueError('Expected TRUE or FALSE values for this value: {0} in line: {1}'
				.format(new_value, new_diff_row))

	elif new_value == 'NONE':
		if field_name in MAX_NORMAL_FIELDS:
			temp_state_row[field_name] = math.inf

		elif field_name in MIN_NORMAL_FIELDS:
			temp_state_row[field_name] = 0

	else:
		try:
			temp_state_row[field_name] = int(new_value)

		except ValueError:
			try:
				temp_state_row[field_name] = float(new_value)	
			
			except ValueError:
				raise ValueError('Unexpected value: {0} in line: {1}'
					.format(new_value, new_diff_row))

	temp_state_row['lockdown_level'] = calc_lockdown_level(temp_state_row)

	all_states_table.append(temp_state_row)

	return temp_state_row


def add_country_row(old_state_row, new_diff_row):
	'''
	@purpose: creates a new country row,
		given a country row in the diff table.
	'''

	new_state_row = old_state_row.copy()

	# takes the seconds value from the previous country state,
	# 	and adds 1 to it, so the order is kept.
	new_state_row['start_date'] = (new_diff_row[CHANGE_DATE_INDEX] +
		timedelta(seconds=old_state_row['start_date'].second + 1))

	return apply_diff(new_state_row, new_diff_row)


def compare_directive_severity(country_row, old_province_row, diff_row):
	'''
	@returns: 1 if country row is more stringent
			 -1 if country row is more easy-going
			  0 if directives are identical
	'''

	field_name = diff_row[CHANGED_FIELD_INDEX]
	country_value = country_row[field_name]
	old_province_value = old_province_row[field_name]

	if old_province_value == None:
		return STRINGENT_COUNTRY
	if country_value == None:
		return EASYGOING_COUNTRY

	if field_name in TF_FIELDS or field_name in LEVELS_FIELDS or field_name in MIN_NORMAL_FIELDS:
		if country_value > old_province_value:
			return STRINGENT_COUNTRY
		elif country_value < old_province_value:
			return EASYGOING_COUNTRY
		else:
			return IDENTICAL_DIRECTIVES

	elif field_name in MAX_NORMAL_FIELDS:
		if country_value > old_province_value:
			return EASYGOING_COUNTRY
		elif country_value < old_province_value:
			return STRINGENT_COUNTRY
		else:
			return IDENTICAL_DIRECTIVES


def is_country_directive_stringent(country_row, old_country_row, diff_row):
	field_name = diff_row[CHANGED_FIELD_INDEX]
	old_country_value = old_country_row[field_name]
	country_value = country_row[field_name]

	if old_country_value == None:
		return True
	if country_value == None:
		return False

	if field_name in TF_FIELDS or field_name in LEVELS_FIELDS or field_name in MIN_NORMAL_FIELDS:
		return country_value > old_country_value
	
	elif field_name in MAX_NORMAL_FIELDS:
		return country_value < old_country_value


def add_province_from_country(country_name, country_row, old_country_row, diff_row):
	'''
	@purpose: creates a new province row in the states table,
		given a country row in the diff table.
		only if the country row has more stringent values,
		a new province row is created, and it inherits those values.
	'''

	for province_name in provinces_in_countries[country_name]:
		old_province_row = find_old_province(country_name, province_name)
		if not old_province_row:
			raise ValueError('Earlier state for the province: "{0}" in country: "{1}" was not found.'
				.format(province_name, country_name))

		if is_country_directive_stringent(country_row, old_country_row, diff_row):
			if STRINGENT_COUNTRY == compare_directive_severity(country_row, old_province_row, diff_row):
				add_province_row(country_row, old_province_row, diff_row)
		else:
			if EASYGOING_COUNTRY == compare_directive_severity(country_row, old_province_row, diff_row):
				add_province_row(country_row, old_province_row, diff_row)


def add_province_row(old_country_row, old_province_row, new_diff_row):
	'''
	@purpose: creates a new province row in the states table,
		given a province row in the diff table.
		first, it combines the previous country and the previous province state.
		the method of combining the two is, for each field,
		picking the more stringent value, aka the most severe directive.
		then, it applies the change from the diff table province row
	'''

	new_province_row = old_province_row.copy()

	# takes the seconds value from both previous country state, and previous province state
	# it finds the max and 1 to it, to keep the new seconds value always higher than previous values.
	new_province_row['start_date'] = (new_diff_row[CHANGE_DATE_INDEX] +
		timedelta(seconds=max(
			old_province_row['start_date'].second,
			old_country_row['start_date'].second
		) + 1)
	)

	apply_diff(new_province_row, new_diff_row)


def find_old_country(country_name):
	'''
	@purpose: finds the previous (old timewise) state in which the same country appeared on.
	'''

	all_states_table.sort(key=lambda item:item['start_date'], reverse=True)
	
	for state in all_states_table:
		if state['province'] == ALL_PROVINCES and state['country'] == country_name:
			return state

	return None


def find_old_province(country_name, province_name):
	'''
	@purpose: finds the previous (old timewise) state in which the same province appeared on.
	'''

	all_states_table.sort(key=lambda item:item['start_date'], reverse=True)
	
	for state in all_states_table:
		if state['country'] == country_name and state['province'] == province_name:
			return state

	return None


def set_province_priority(state_row):
	'''
	@purpose: upper 'all' values in province, while lowering others.
		This solves the situation where in a given day, both a country and a province has new state.
		This means if the province is added earlier, it will not inherit the country's directives.
		Hence, the 'all' values will appear ealier after the alphabetical ordering, which solves the issue.
	'''

	if ALL_PROVINCES == state_row[PROVINCE_INDEX]:
		return state_row[PROVINCE_INDEX].lower()

	return state_row[PROVINCE_INDEX].upper()


def get_diff_paths(diff_tables_dir_path):
	'''
	@returns: list of all csv file paths within a specific directory
	'''

	return [os.path.join(diff_tables_dir_path, file)
		for file in os.listdir(diff_tables_dir_path)
		if file.endswith('.csv')]


def get_csv_rows(diff_path):
	'''
	@returns: a list of records (lists) from a diff table
	'''

	with open(diff_path, 'rt') as f:
		diff_reader = csv.reader(f, delimiter=',')
		return list(diff_reader)[1:]


def add_missing_areas(diff_row):
	'''
	@purpose: looping over all the diff rows first,
		to add the areas which were not added manually to the external areas file
	'''

	country_name = diff_row[COUNTRY_INDEX]
	province_name = diff_row[PROVINCE_INDEX]

	if not find_old_country(country_name):
		print(WARNING_MISSING_COUNTRY.format(country_name))

		create_initial_state(country_name, ALL_PROVINCES)

	if province_name.lower() != ALL_PROVINCES:
		if not find_old_province(country_name, province_name):
			print(WARNING_MISSING_PROVINCE.format(province_name, country_name))
			create_initial_state(country_name, province_name)

			if country_name in provinces_in_countries:
				provinces_in_countries[country_name].append(province_name)
			else:
				provinces_in_countries[country_name] = [province_name]


def validate_diff_row(diff_row):
	field_name = diff_row[CHANGED_FIELD_INDEX]
	prev_value = diff_row[PREV_FIELD_INDEX]
	new_value = diff_row[NEW_VALUE_INDEX]

	if prev_value == new_value:
		print(f'Warning: Same value {prev_value} entered\nIn diff row: {diff_row}')

	if field_name in TF_FIELDS:
		if prev_value not in VALID_TF_INPUTS:
			print(f'Warning: "changed_from" value: {prev_value}\nIn diff row: {diff_row}')
		if new_value not in VALID_TF_INPUTS:
			raise ValueError(f'Error: "changed_to" value: {new_value}\nIn diff row: {diff_row}')

	elif field_name in LEVELS_FIELDS:
		if prev_value not in VALID_LEVELS_INPUTS:
			print(f'Warning: "changed_from" value: {prev_value}\nIn diff row: {diff_row}')
		if new_value not in VALID_LEVELS_INPUTS:
			raise ValueError(f'Error: "changed_to" value: {new_value}\nIn diff row: {diff_row}')

	elif field_name in MAX_NORMAL_FIELDS or field_name in MIN_NORMAL_FIELDS:
		if prev_value != 'NONE':
			try:
				int(prev_value)
			except ValueError:
				try:
					float(prev_value)	
				except ValueError:
					raise ValueError(f'Warning: "changed_from" value: {prev_value}\nIn diff row: {diff_row}')

		if new_value != 'NONE':
			try:
				int(new_value)
			except ValueError:
				try:
					float(new_value)	
				except ValueError:
					raise ValueError(f'Error: "changed_to" value: {new_value}\nIn diff row: {diff_row}')
	
	else:
		print(f'Error: Unknown "changed_field": {field_name}\nIn diff row: {diff_row}')


def process_diff_row(diff_row):
	'''
	@purpose: main function for converting a row in the diff table to a row in states table.
	@note: there are two types of rows considered here: country and province.
		for country rows we take the previous row for
		the same country and update it to create the new state.
		province rows can be created in two ways:
			1. A province row in the diff table.
			2. A country row in the diff table, which is more stringent
				than the current condition in the province.
		in both ways we take the previous province row
		and the previous row of the matching country.
		we combine those two, picking the more stringent values.
	'''

	country_name = diff_row[COUNTRY_INDEX]
	province_name = diff_row[PROVINCE_INDEX]

	old_country_row = find_old_country(country_name)
	if not old_country_row:
		raise IOError(ERROR_MISSING_COUNTRY.format(country_name))
		
	if province_name.lower() == ALL_PROVINCES:   # country directive
		country_row = add_country_row(old_country_row, diff_row)

		if country_name in provinces_in_countries:
			add_province_from_country(country_name, country_row, old_country_row, diff_row)
	
	else:   # province directive
		old_province_row = find_old_province(country_name, province_name)
		if not old_province_row:
			raise IOError(ERROR_MISSING_PROVINCE.format(province_name, country_name))

		add_province_row(old_country_row, old_province_row, diff_row)


def add_default_states(explored_areas_path):
	'''
	@purpose: adds initial states for all countries
		and provinces to the states table (1/1/2019)
	'''

	areas_rows = get_csv_rows(explored_areas_path)
	for row in areas_rows:
		country_name, province_name = row
		create_initial_state(country_name, province_name)
		
		if province_name != ALL_PROVINCES:
			if country_name in provinces_in_countries:
				if province_name not in provinces_in_countries[country_name]:
					provinces_in_countries[country_name].append(province_name)
			else:
				provinces_in_countries[country_name] = [province_name]


def diffs_to_states(diff_tables_dir_path, explored_areas_path):
	'''
	@purpose: the base function.
		reads the diff tables from csv files,
		and sends it to further processing.
	'''

	add_default_states(explored_areas_path)

	diff_paths = get_diff_paths(diff_tables_dir_path)

	for diff_path in diff_paths:
		diff_rows = get_csv_rows(diff_path)
		for row in diff_rows:
			try:
				row[CHANGE_DATE_INDEX] = datetime.strptime(row[CHANGE_DATE_INDEX], DATE_FORMAT)
			except ValueError as e:
				print(row, e)

		sorted_diff_rows = sorted(diff_rows, key=set_province_priority)
		sorted_diff_rows.sort(key=lambda item:item[CHANGE_DATE_INDEX])

		# first, go through all records to get all province names.
		# this is done so provinces which are omitted from the explored_areas file, will be considered from the beginning
		for diff_row in sorted_diff_rows:
			add_missing_areas(diff_row)

		for diff_row in sorted_diff_rows:
			validate_diff_row(diff_row)
			process_diff_row(diff_row)


def remove_seconds():
	'''
	@purpose: removes the seconds from start date column, which assist ordering the data.
	'''

	for state in all_states_table:
		state['start_date'] = state['start_date'].replace(second=0)


def remove_duplicates(list_of_dicts):
	'''
	@purpose: removes duplicate rows of the same country/province within the same day.
		keeps only the most updated occurence of the day.
	'''

	temp_index = 0
	for index in range(len(list_of_dicts) - 1):
		curr_state = list_of_dicts[temp_index]
		curr_date = (
			curr_state['start_date'].day,
			curr_state['start_date'].month,
			curr_state['start_date'].year
		)

		next_state = list_of_dicts[temp_index + 1]
		next_date = (
			next_state['start_date'].day,
			next_state['start_date'].month,
			next_state['start_date'].year
		)

		if (curr_state['country'] == next_state['country'] and
			curr_state['province'] == next_state['province'] and
			curr_date == next_date):
			list_of_dicts.remove(next_state)
		else:
			temp_index += 1


def export_to_csv(list_of_dicts, output_path):
	keys = list_of_dicts[0].keys()
	with open(output_path, 'w', encoding='utf8', newline='') as output_file:
		fc = csv.DictWriter(output_file, fieldnames=list_of_dicts[0].keys())
		fc.writeheader()
		fc.writerows(list_of_dicts)


def fix_data(table):
	'''
	@purpose: fixes the data:
		1. coverts inf records to None in 'congregation_restriction'
		2. coverts 0 records to None in 'distance_saving_instructions'
	'''

	for row in table:
		for min_normal_field in MIN_NORMAL_FIELDS:
			if row[min_normal_field] == 0:
				row[min_normal_field] = None
		for max_normal_field in MAX_NORMAL_FIELDS:
			if row[max_normal_field] == math.inf:
				row[max_normal_field] = None


def get_arguments():
	parser = argparse.ArgumentParser(description=PROGRAM_DESCRIPTION)
	parser.add_argument('-i', help=INPUT_HELP, required=True)
	parser.add_argument('-c', help=AREAS_HELP, required=True)
	parser.add_argument('-o', help=OUTPUT_HELP, required=True)
	return parser.parse_args()


def modify_data():
	if not TEST_MODE:
		remove_seconds()

	fixed_all_states_table = sorted(all_states_table, key=lambda item:item['province'])
	fixed_all_states_table = sorted(fixed_all_states_table, key=lambda item:item['country'])
	fixed_all_states_table.sort(key=lambda item:item['start_date'], reverse=True)

	if not TEST_MODE:
		remove_duplicates(fixed_all_states_table)
		fix_data(fixed_all_states_table)

	return fixed_all_states_table


def main():
	args = get_arguments()
	diff_tables_dir_path = args.i
	explored_areas_path = args.c
	state_table_path = args.o

	diffs_to_states(diff_tables_dir_path, explored_areas_path)

	fixed_all_states_table = modify_data()

	export_to_csv(fixed_all_states_table, state_table_path)


if __name__ == '__main__':
	main()