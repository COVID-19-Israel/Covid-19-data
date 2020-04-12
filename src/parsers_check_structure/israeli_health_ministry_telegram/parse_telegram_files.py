import os
import logging
import sys

# TODO:
sys.path.append("../files_tables_parser")

from logger import create_log
import parsers as p

# TODO:

FILES_DIR = r"../../data/other/israeli_health_ministry_telegram_data/telegram_files"

OUTPUT_DIR = r"../../data/other/israeli_health_ministry_telegram_data/israeli_telegram_csv_files"


def main():
    """

    :return:
    """
    create_log()
    counter = 1
    for f in os.listdir(FILES_DIR):
        path = os.path.join(FILES_DIR, f)
        logging.info(f"{counter}: started parsing {os.path.basename(path)}")
        parser = p.FileParser(path, OUTPUT_DIR)
        parser.run()
        counter += 1


if __name__ == "__main__":
    main()
