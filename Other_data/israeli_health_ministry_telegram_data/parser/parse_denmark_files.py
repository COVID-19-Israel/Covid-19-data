import os
import wget
import parsers
import datetime

try:
    from googlesearch import search
except ImportError:
    print("No module named 'google' found")

START_DATE = 3

DOWNLOADED_FILES_PATH = '..\\denmark_files\\'
DENMARK_FILE_PREFIX = 'covid19-overvaagningsrapport-'
PDF_SUFFIX = '.pdf'

def main():
    for day in range(START_DATE, datetime.date.today().day):
        today = datetime.datetime(2020,4,day).strftime("%d%m%Y")
        file_query = DENMARK_FILE_PREFIX+today
        for file in search(file_query, num=1, stop=1):
            print(f"file to download: {file}")
            downloaded_file_path = DOWNLOADED_FILES_PATH + DENMARK_FILE_PREFIX + today + PDF_SUFFIX
            wget.download(file, downloaded_file_path)
            print(f"downloaded file is: {downloaded_file_path}")
            parser = parsers.FileParser(downloaded_file_path, file_date=datetime.datetime(2020,4,day).strftime("%Y-%m-%d"))
            print (f'{day - START_DATE + 1}: started parsing {os.path.basename(downloaded_file_path)}, file is: {file}')
            parser.run()


if __name__ == '__main__':
    main()