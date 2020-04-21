import os
import logging
import sys
import cities_parser as cp

sys.path.append("../files_tables_parser")

from logger import create_log
import parsers as p


FILES_DIR = r"./telegram_files"
OUTPUT_DIR = r"../../data/other/israeli_health_ministry_telegram_data/csv"


def main():
    """

    :return:
    """
    create_log(logging.DEBUG)
    logging.info(f"~~~~~~~ Starts parsing files from {FILES_DIR} ~~~~~~~")
    counter = 1
    for f in os.listdir(FILES_DIR)[::-1]:
        path = os.path.join(FILES_DIR, f)
        logging.info(f"{counter}: started parsing {os.path.basename(path)}")
        try:
            parser = p.FileParser(path, OUTPUT_DIR)
            parser.run()
        except Exception:
            logging.error(f'Failed to run General FileParser on the file: {os.path.basename(path)}')
        cities_parser = cp.CitiesFileParser(path, OUTPUT_DIR)
        cities_parser.run()

        counter += 1

    logging.info("~~~~~~~ FINISHED PARSING FILES ~~~~~~~")


if __name__ == "__main__":
    main()
