import os
import sys
import wget
import datetime
import pandas as pd
from dateutil.rrule import rrule, DAILY
import logging

sys.path.append('../files_tables_parser')
import parsers
from logger import create_log

try:
    from googlesearch import search
except ImportError:
    print("No module named 'google' found")

START_DATE = datetime.date(2020,4,3)

DOWNLOADED_FILES_PATH = 'denmark_files\\'
DENMARK_FILE_PREFIX = '_covid19-overvaagningsrapport-'
PDF_SUFFIX = '.pdf'

OUTPUT_DIR = '..\\..\\data\\other\\denmark_daily_updates_data\\'
TABLES_OUTPUT_DIR = os.path.join(OUTPUT_DIR, 'files_tables\\')
DB_STREAM_TABLES_OUTPUT_DIR = os.path.join(OUTPUT_DIR, 'db_stream_tables\\')
DB_STREAM_TABLES_OUTPUT_FILE_NAME = '_tables_conclusion.csv'

TABLES_AMOUNT = 4

LEFT_TABLE_SUFFIX = '_x'
RIGHT_TABLE_SUFFIX = '_y'
DEFAULT_ADDED_COLUMN_NAME = 'key_0'

REMOVED_DATA_AFTER_SPACE_COLUMNS = ['Dead (%)']

DB_STREAM_TABLE_WANTED_FIELDS = ['Region',
                                 'Hospitalized',
                                 'Critical',
                                 'Ventilated',
                                 'Confirmed COVID-19 cases',
                                 'Dead',
                                 'Number of people tested',
                                 'Population']

def merge_tables_by_first_col(tables):
    """
    This function merges a list of tables into one dataframe based on the first col of each table.
    :param tables - the list of tables
    :return: the merged table's dataframe
    """
    for table_index, table in enumerate(tables):
        table_df = pd.DataFrame(columns=table[0], data=table[1:])
        if 0 == table_index:
            merged_table = pd.concat([table_df])
        else:
            if merged_table.columns[0] != table_df.columns[0]:
                merged_table = merged_table.merge(table_df,
                                                  left_on=merged_table[merged_table.columns[0]],
                                                  right_on=table_df[table_df.columns[0]],
                                                  how='left')
                # erases the column that used as the merged column of the new table
                del merged_table[table_df.columns[0]]

            else:
                merged_table = merged_table.merge(table_df,
                                                  on=merged_table[merged_table.columns[0]],
                                                  how='left')
                del merged_table[table_df.columns[0] + RIGHT_TABLE_SUFFIX]
                merged_table.rename(columns={(table_df.columns[0] + LEFT_TABLE_SUFFIX):
                                                 table_df.columns[0]},
                                    inplace=True)

            if DEFAULT_ADDED_COLUMN_NAME == merged_table.columns[0]:
                del merged_table[merged_table.columns[0]]
    return merged_table


def remove_values_after_string(df, columns, substr):
    """
    This function removes the values in the given columns after a given sub-string from a given dataframe.
    :param df - the given dataframe
    :param columns - the given columns
    :param substr - the given sub-string
    :return: the dataframe without the removed values
    """
    for column in columns:
        try:
            for col_index in range(len(df[column])):
                if str == type(df[column][col_index]):
                    df[column].replace(df[column][col_index],
                                       df[column][col_index].split(substr)[0],
                                       inplace=True)
        except KeyError:
            raise KeyError(f'{column} is not a header in the table')

        df.rename(columns={column: str(column).split(substr)[0]}, inplace=True)
    return df


def remove_unnecessary_fields(df):
    """
    This function removes all unnecessary fields from a dataframe
    (if there are duplicates, takes the max value of each value in the field).
    :param df - the dataframe
    :return: the dataframe without the unnecessary fields.
    """
    # Finds all duplicated columns
    duplicated_columns = [column for column in df.columns
                          if column.endswith(LEFT_TABLE_SUFFIX)
                          and (column.replace(LEFT_TABLE_SUFFIX,'') + RIGHT_TABLE_SUFFIX) in df.columns]

    # Creates a merged column that contains the max value of each value in the columns
    for column in duplicated_columns:
        original_column = column.replace(LEFT_TABLE_SUFFIX, '')
        df[original_column + LEFT_TABLE_SUFFIX].fillna('0', inplace=True)
        df[original_column + RIGHT_TABLE_SUFFIX].fillna('0', inplace=True)
        df[original_column] = df[[original_column + LEFT_TABLE_SUFFIX,
                                  original_column + RIGHT_TABLE_SUFFIX]].max(axis=1)

    unnecessary_fields = [column for column in df.columns if column not in DB_STREAM_TABLE_WANTED_FIELDS]
    return df.drop(columns=unnecessary_fields)


def main():
    create_log(logging.DEBUG)
    os.makedirs(DOWNLOADED_FILES_PATH, exist_ok=True)
    days_counter = 0
    for date in rrule(DAILY, dtstart=START_DATE, until=datetime.date.today()):
        days_counter += 1
        file_query = DENMARK_FILE_PREFIX + date.strftime("%d%m%Y")
        logging.info(f'started parsing date: {date.strftime("%d-%m-%Y")}')
        for file in search(file_query, num=1, stop=1):
            downloaded_file_path = DOWNLOADED_FILES_PATH + date.strftime("%Y-%m-%d") + file_query + PDF_SUFFIX
            if os.path.basename(downloaded_file_path) not in os.listdir(DOWNLOADED_FILES_PATH):
                logging.info(f"start downloading: {file}")
                wget.download(file, downloaded_file_path)
                logging.info(f"downloaded file is: {downloaded_file_path}")
            parser = parsers.FileParser(downloaded_file_path, TABLES_OUTPUT_DIR)
            logging.info(f'{days_counter}: started parsing {os.path.basename(downloaded_file_path)}, file is: {file}')
            tables = parser.run()
            if tables:
                if TABLES_AMOUNT != len(tables):
                    raise ValueError(f'Parse error with the file: {downloaded_file_path}')

                # makes the regions table that I want it to be the init table the first table in the list
                tables[0], tables[1] = tables[1], tables[0]
                merged_table = merge_tables_by_first_col(tables)
                full_fields_table = remove_values_after_string(merged_table, REMOVED_DATA_AFTER_SPACE_COLUMNS, ' ')
                parsed_table = remove_unnecessary_fields(full_fields_table)
                parsed_table.to_csv(DB_STREAM_TABLES_OUTPUT_DIR
                                    + date.strftime('%d-%m-%Y')
                                    + DB_STREAM_TABLES_OUTPUT_FILE_NAME,
                                    index=False)

    logging.info("~~~~~~~ FINISHED PARSING FILES ~~~~~~~")


if __name__ == '__main__':
    main()