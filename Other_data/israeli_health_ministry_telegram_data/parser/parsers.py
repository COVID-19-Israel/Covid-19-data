import tabula
from pptx import Presentation
import os
import pandas as pd
import json
from translate import Translator

FIELD_SEP = '@@@'
CSV_SUFFIX = '.csv'
SPECIFIC_TABLE_PREFIX = '_table_no_'
CITIES_COLUMNS = {'ישוב', 'אוכלוסיה נכון ל 2018-', 'מספר חולים'}
DOWNLOADED_FILES_DICT_PATH = ".json"

class FileParser:

    PPTX_SUFFIX = 'pptx'
    PDF_SUFFIX = 'pdf'

    def __init__(self, path):
        self.path = path
        self._data = list()

    def run(self):
        # TODO: if the file already parsed- skip.
        file_suffix = self.path.split('.')[-1]

        if FileParser.PPTX_SUFFIX == file_suffix:
            parser = PptxParser(self.path)
        elif FileParser.PDF_SUFFIX == file_suffix:
            parser = PdfParser(self.path)
        else:
            raise ValueError("This class don't parse files of the type: {}, path of the file: {}"
                             .format(file_suffix, self.path))

        parser.parse_file()
        parser.export_to_csv()

    def parse_file(self):
        raise NotImplementedError

    def _create_output_file_path(self, table_index):
        """

        :param table_index:
        :return:
        """
        file_name = os.path.basename(self.path)
        file_name = "".join(file_name.split(".")[:-1])
        output_file_name = ''.join([file_name, SPECIFIC_TABLE_PREFIX, str(table_index), CSV_SUFFIX])
        with open(output_file_name, mode='w+'):
            pass
        return output_file_name

    def export_to_csv(self):
        """
        gets a list of tables (table = matrix)
        exports each one to different csv
        :return: None
        """
        # need to make sure that row and column index are removed.
        for table_index, table in enumerate(self._data, start=1):
            table_df = pd.DataFrame(table)
            table_df["Date"] = FileParser._get_file_date(os.path.basename(self.path))
            output_file_name = self._create_output_file_path(table_index)
            table_df.to_csv(output_file_name, index=False, encoding='utf-8')

    @staticmethod
    def _get_file_date(filename):
        with open(DOWNLOADED_FILES_DICT_PATH, "r") as f:
            downloaded_files_dict = json.load(f)
        return downloaded_files_dict[filename]


class PptxParser(FileParser):
    @staticmethod
    def _extract_data_from_cell(table_cell):
        translator = Translator(to_lang='en', from_lang='he')
        data = list()
        cell_paragraphs = table_cell.text_frame.paragraphs
        for cell_paragraph in cell_paragraphs:
            for run in cell_paragraph.runs:
                data.append(translator.translate(run.text))
        return ' '.join(data)

    def parse_file(self):
        prs = Presentation(self.path)
        prs_tables = list()
        for slide in prs.slides:
            for shape in slide.shapes:
                if not shape.has_table:
                    continue
                parsed_table = list()
                tbl = shape.table
                for row_index in range(0, len(tbl.rows)):
                    tbl_row = list()
                    for col_index in range(0, len(tbl.columns)):
                        table_cell = tbl.cell(row_index, col_index)
                        cell_data = PptxParser._extract_data_from_cell(table_cell)
                        tbl_row.append(cell_data)
                    parsed_table.append(tbl_row)
                prs_tables.append(parsed_table)
        self._data = prs_tables


class PdfParser(FileParser):
    def parse_file(self):
        pdf_tables = tabula.read_pdf(input_path=self.path,
                                     pages="all",
                                     stream=True,
                                     silent=True)
        for table_df in pdf_tables:
            # the loop will work only in the first case, where the headers are correct.
            self._check_cities_pdf(table_df)

    def _check_cities_pdf(self, table_df):
        """
        table type check: checks if the headers match the cities format.
        THIS IS AN EXAMPLE FOR A TABLE TYPE CHECK.
        :param table_df:
        :return: None
        """
        if CITIES_COLUMNS.issubset(set(table_df.columns.tolist())):
            parser = CitiesPdfParser(self.path)
            self._data.append(parser.parse_file())



class CitiesPdfParser(FileParser):

    @staticmethod
    def _translate_city():
        """
        upload cached cities-names translate dict
        :return:
        """
        with open(r".\cities_dict.json", "r") as f:
            return json.load(f)

    def parse_file(self):
        """

        parses the tables from the pdf files from MOH telegram.
        the original tables has hebrew city name, population and infected number.
        :param data:
        :return:
        """
        pdf_tables = tabula.read_pdf(input_path=self.path,
                                     pages="all",
                                     stream=True,
                                     silent=True)
        fixed_data = []
        headers = ["City_Name", "Population", "Infected"]
        fixed_data.append(headers)

        for data_df in pdf_tables:
            data_df = data_df.where(pd.notnull(data_df), None)
            list_data = data_df.values.tolist()
            cityname_translator = self._translate_city()

            for line in list_data:
                if line[1] is not None:
                    if line[1].replace(",", "").isdigit():
                        fixed_data.append([cityname_translator[line[0]], line[1].replace(",", ""), str(line[2]).replace(",", "")])
                else:
                    fixed_data.append([cityname_translator[line[0]], line[2].replace(",", ""), str(line[3]).replace(",", "")])
        return fixed_data

