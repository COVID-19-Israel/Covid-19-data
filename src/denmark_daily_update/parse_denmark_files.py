import os
import sys
import wget
import datetime
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

def main():
    create_log()
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
            parser = parsers.FileParser(downloaded_file_path, OUTPUT_DIR)
            logging.info(f'{days_counter}: started parsing {os.path.basename(downloaded_file_path)}, file is: {file}')
            parser.run()
    logging.info("~~~~~~~ FINISHED PARSING FILES ~~~~~~~")


if __name__ == '__main__':
    main()