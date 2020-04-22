import tabula
import os
import sys
import pandas as pd
import re

sys.path.append("../files_tables_parser")

from parser_translator import ParserTranslator

CSV_SUFFIX = ".csv"
PDF_SUFFIX = "pdf"
CITIES_FILE_IDENTIFIER = "ישובים"
DAILY_UPDATE_OUTPUT_DIR = "cities_new"
EXCLUDE_STRING = 'ערבים'
ADDITIONAL_FILES = ['2020-04-05_ללא כותרת', '2020-04-20_דוח חדש כלל הארץ - 20.04.20']
OLD_SCRIPT_FILES = ['2020-04-03','2020-04-05','2020-04-06','2020-04-07',
                   '2020-04-09','2020-04-11',]


def format_int(value: str) -> int:
    try:
        return(int(float(value)))
    except:
        if value in ['','nan', None]:
            return None
        else:
            value = value.strip().replace(',', '').replace('+', '')
            return None if value == '' else int(float(value))

def get_date_from_filename(file_name):
    path_parts = re.split("[_.]", file_name)
    date = path_parts[0]
    return date

class CitiesFileParser:
    def __init__(self, path, output_dir):
        self.output_dir = os.path.join(output_dir,DAILY_UPDATE_OUTPUT_DIR)
        self.path = path
        self._data = None

    def run(self):
        file_name = os.path.basename(self.path).split(".")
        file_suffix = file_name[-1]
        file_name = "".join(file_name[:-1])
        self.file_name = file_name
        if (os.path.exists(self._create_output_file_path())):
            return
        if file_suffix == PDF_SUFFIX:
            if (CITIES_FILE_IDENTIFIER in file_name or file_name in ADDITIONAL_FILES) \
                    and EXCLUDE_STRING not in file_name:
                if get_date_from_filename(file_name) in OLD_SCRIPT_FILES:
                    self.parse_pdf_file_old()
                else:
                    self.parse_pdf_file()

                self.export_to_csv()

    def parse_pdf_file(self):
        pdf_tables = tabula.read_pdf(input_path=self.path,
                                     pages="all",
                                     stream=True,
                                     silent=True)
        is_first_iteration = True
        top_to_bottom = True
        parsed_table = list()

        first_headers_len = len(pdf_tables[0].columns)
        if len(pdf_tables[0].columns) == 3:
            # Don't add initial unneeded table
            first_headers_len = len(pdf_tables[1].columns)

        for idx,pdf_table in enumerate(pdf_tables):
            if len(pdf_table.columns) != first_headers_len:
                # Don't add this table.
                continue
            concated_table = list()
            pdf_table = pdf_table.where(pd.notnull(pdf_table), None)
            table_headers = [header if 'Unnamed' not in str(header)
                             else None
                             for header in pdf_table.keys()]
            if list(range(len(table_headers))) != table_headers:
                concated_table.append(table_headers)
            [concated_table.append(row) for row in pdf_table.values.tolist()]
            if is_first_iteration:
                top_to_bottom = not (None in concated_table[0] and any(concated_table[0]))  # row is not empty
                is_first_iteration = False
            CitiesFileParser._concat_empty_lines(concated_table, is_col_header=True, top_to_bottom=top_to_bottom)

            for row in CitiesFileParser._translate_table(concated_table):
                parsed_table.append(row)

        self._data = pd.DataFrame(columns=parsed_table[0], data=parsed_table[1:])

    def parse_pdf_file_old(self):
        translator = ParserTranslator(to_lang="en", from_lang="he")
        pdf_tables = tabula.read_pdf(
            input_path=self.path, pages="all", stream=True, silent=True
        )

        fixed_data = []
        headers = ["City_Name", "Population", "Infected"]
        fixed_data.append(headers)

        for data_df in pdf_tables:
            data_df = data_df.where(pd.notnull(data_df), None)
            list_data = data_df.values.tolist()

            for line in list_data:
                # Solves the problem that the data moved one column right in the middle of the file
                if line[1] is not None:
                    if type(line[1])==int:
                        fixed_data.append(
                            [
                                translator.translate_word(line[0]),
                                line[1],
                                str(line[2]).replace(",", ""),
                            ]
                        )
                    elif line[1].replace(",", "").isdigit():
                        fixed_data.append(
                            [
                                translator.translate_word(line[0]),
                                line[1].replace(",", ""),
                                str(line[2]).replace(",", ""),
                            ]
                        )
                else:
                    fixed_data.append(
                        [
                            translator.translate_word(line[0]),
                            line[2].replace(",", ""),
                            str(line[3]).replace(",", ""),
                        ]
                    )
        self._data = pd.DataFrame(columns=fixed_data[0], data=fixed_data[1:])

    @staticmethod
    def _translate_table(translated_table, to_lang="en", from_lang="he"):
        """
        This function translates a table from danish to english (only the top line and the bottom line
        :param translated_table - the table that need to be translated
        :return: the translated table
        """
        translator = ParserTranslator(to_lang=to_lang, from_lang=from_lang)
        for row_index in range(0, len(translated_table)):
            for col_index in range(len(translated_table[0])):
                translated_table[row_index][col_index] = translator.translate_word(
                    str(translated_table[row_index][col_index]))
        return translated_table

    @staticmethod
    def _merge_completed_lines(merged_table, top_to_bottom, is_col_header):
        """
        This function merges all of the following rows that can complete each other in a table.
        :param merged_table - the table that its rows need to be merged.
        :param top_to_bottom - is the direction of the table is from top to bottom.
        :param is_col_header - is there a header column in the table.
        :return: None.
        """
        merged_row = list()
        start_index = len(merged_table) - 2
        end_index = 0
        index_jumps = -1

        if merged_table:
            values_end_col = len(merged_table[0])

        if is_col_header:
            values_end_col -= 1

        if top_to_bottom:
            start_index = 1
            end_index = len(merged_table) - 1
            index_jumps = 1

        row_index = start_index

        for i in range(start_index, end_index, index_jumps):
            merged_row = CitiesFileParser._are_rows_completed(merged_table, row_index,(-1 * index_jumps), values_end_col)
            if merged_row:
                merged_table[row_index - index_jumps] = merged_row
                merged_table.remove(merged_table[row_index])
                if 0 > index_jumps:
                    row_index += index_jumps
            else:
                row_index += index_jumps

    @staticmethod
    def _are_rows_completed(matrix, row_index, previous_row_offset, values_end_col):
        """
        This function checks if the following rows can complete each other.
        If they are, it returns the merged row, else it returns an empty list
        :param matrix - the table that contains the rows.
        :param row_index - the index of the first row.
        :param pervious_row_offset - the offset between the indexes of the rows.
        :param values_end_col - the column that the values ends at (if there is any header column in the table).
        :return: merged row if the rows can be merged, else an enpty list.
        """
        merged_row = list()
        if 0 > previous_row_offset:
            min_index = row_index + previous_row_offset
            max_index = row_index
        else:
            min_index = row_index
            max_index = row_index + previous_row_offset
        merged_values = zip(matrix[row_index][:values_end_col],
                            matrix[row_index + previous_row_offset][:values_end_col])
        for values in merged_values:
            if None not in values:
                return list()
            merged_row.append(values[1 - values.index(None)])

        for headers in zip(matrix[min_index][values_end_col:], matrix[max_index][values_end_col:]):
            merged_row.append((' '.join([str(headers[0]), str(headers[1])])
                               .replace('None ', '')).replace(' None', ''))

        return merged_row

    @staticmethod
    def _concat_empty_lines(concated_table, is_col_header, top_to_bottom=True):
        """
        This function concats empty lines that made because of line-break in
        the table to the line before (or after, in case of empty line in the index 0)
        :param concated_table - the table that need to be concated
        :return: None
        """
        CitiesFileParser._merge_completed_lines(concated_table, top_to_bottom, is_col_header)

        row_index = 1
        for i in range(1, len(concated_table)):
            try:
                if (None in concated_table[row_index]
                    or 'None' in concated_table[row_index]
                    or (None in concated_table[0] and 1 == row_index)):
                    full_fields = zip(concated_table[row_index - 1], concated_table[row_index])
                    for col_index, full_field in enumerate(full_fields):
                        concated_table[row_index - 1][col_index] = (' '.join([str(full_field[0]),
                                                                              str(full_field[1])])
                                                                    .replace('None ', '')).replace(' None', '')
                    concated_table.remove(concated_table[row_index])
                else:
                    row_index += 1
            except Exception:
                print(f"i: {i}, row_index: {row_index}, table: {concated_table}")

    def _create_output_file_path(self):
        file_name = os.path.basename(self.path)
        file_name = "".join(file_name.split(".")[:-1])
        file_name = get_date_from_filename(file_name)
        return os.path.join(self.output_dir,file_name)+CSV_SUFFIX

    def export_to_csv(self):
        os.makedirs(self.output_dir, exist_ok=True)
        output_file_name = self._create_output_file_path()
        self._data.to_csv(output_file_name, index=False, encoding="utf-8")
