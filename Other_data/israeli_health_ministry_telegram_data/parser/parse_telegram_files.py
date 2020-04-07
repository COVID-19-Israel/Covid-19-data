import parsers as p
import os
import logging
from logger import create_log


FILES_DIR = r"..\telegram_files\tmp"


def main():
    """

    :return:
    """
    create_log()
    counter = 1
    for f in os.listdir(FILES_DIR):
        path = os.path.join(FILES_DIR, f)
        logging.info(f"{counter}: started parsing {os.path.basename(path)}")
        parser = p.FileParser(path)
        parser.run()
        counter += 1


if __name__ == '__main__':
    main()