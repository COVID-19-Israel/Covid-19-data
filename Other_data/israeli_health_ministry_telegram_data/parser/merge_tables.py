import os
import csv


PROGRAM_DESCRIPTION = 'merges tables from different dates to one table'
INPUT_HELP = 'path of input dir'
OUTPUT_HELP = 'path of output file'

HEADER_INDEX = 1

CITIES_DIR_PATH = 'output\\cities\\'
DAILY_UPDATE_PATH = 'output\\daily_update\\'
OUTPUT_PATH = 'output\\merged\\merge_table.csv'


def get_csv_paths(csv_dir_path):
	return [os.path.join(csv_dir_path, file)
		for file in os.listdir(csv_dir_path)
		if file.endswith('.csv')]


def get_csv_rows(csv_file_path):
	with open(csv_file_path, 'rt') as f:
		csv_reader = csv.reader(f, delimiter=',')
		return list(csv_reader)[HEADER_INDEX:]


def merge_tables(input_dir_path):
	merged_table = []

	csv_paths = get_csv_paths(input_dir_path)
	for csv_path in csv_paths:
		csv_rows = get_csv_rows(csv_path)
		header_row = csv_rows[0]
		info_rows = csv_rows[1:]
		
		if not merged_table:
			merged_table.append(header_row)

		for row in info_rows:
			merged_table.append(row)

	return merged_table


def export_table(merged_table, output_file_path):
	with open(output_file_path, 'w', newline='') as output_file:
	    writer = csv.writer(output_file)
	    writer.writerows(merged_table)


def main():
	merged_table = merge_tables(CITIES_DIR_PATH)
	export_table(merged_table, OUTPUT_PATH)


if __name__ == '__main__':
	main()