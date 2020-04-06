import parsers as p
FILES_DIR = r"..\telegram_files"
import os

def main():
    """

    :return:
    """
    # TODO: LOGGING
    counter = 1
    for f in os.listdir(FILES_DIR):
        path = os.path.join(FILES_DIR, f)
        print(f"{counter}: started parsing {os.path.basename(path)}")
        parser = p.FileParser(path)
        parser.run()
        counter += 1





if __name__ == '__main__':
    main()
