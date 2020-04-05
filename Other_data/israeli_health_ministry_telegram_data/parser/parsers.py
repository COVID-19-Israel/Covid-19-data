import tabula
from pptx import Presentation

import pandas as pd

from translate import Translator

FIELD_SEP = '@@@'
CSV_SUFFIX = '.csv'
SPECIFIC_TABLE_PREFIX = '_table_no_'

class FileParser:

    PPTX_SUFFIX = 'pptx'
    PDF_SUFFIX = 'pdf'

    def __init__(self, path):
        self.path = path
        self.data = str()

    def run(self):
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
        file_name = self.path.split('\\')[-1]
        file_name = file_name.split('/')[-1]
        file_name = file_name.split('.')[0]
        output_file_name = ''.join([file_name, SPECIFIC_TABLE_PREFIX, str(table_index), CSV_SUFFIX])
        with open(output_file_name, mode='w+'):
            pass
        return output_file_name

    def export_to_csv(self):
        for table_index, table in enumerate(self.data, start=1):
            table_df = pd.DataFrame(table)
            output_file_name = self._create_output_file_path(table_index)
            table_df.to_csv(output_file_name, index=False, encoding='utf-8')


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
                for row_index in range(0,len(tbl.rows)):
                    tbl_row = list()
                    for col_index in range(0, len(tbl.columns)):
                        table_cell = tbl.cell(row_index, col_index)
                        cell_data = PptxParser._extract_data_from_cell(table_cell)
                        tbl_row.append(cell_data)
                    parsed_table.append(tbl_row)
                prs_tables.append(parsed_table)
        self.data = prs_tables

class PdfParser(FileParser):
    def parse_file(self):
        translator = Translator(to_lang='en', from_lang='he')
        parsed_pdf_tables = list()
        pdf_tables = tabula.read_pdf(self.path, pages = "all", multiple_tables = True)
        for table in pdf_tables:
            table = table.where(pd.notnull(table), None)
            parsed_table = table.values.tolist()
            for row_index in range(len(parsed_table)):
                for col_index in range(len(parsed_table[0])):
                    if parsed_table[row_index][col_index] is not None:
                        parsed_table[row_index][col_index] = translator.translate(str(parsed_table[row_index][col_index]))
            parsed_pdf_tables.append(parsed_table)
        self.data = parsed_pdf_tables