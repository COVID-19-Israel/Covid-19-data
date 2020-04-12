# Mistry of Health Telegram Files Parser
This is a Telegram bot and a files parser. 
the bot dowloads files by a list of terms, and the parser recognize specific structures, parses the tables in the files and saves them as in csv format. 

## The Telegram Bot
* Runs on the MOHreport telegram channel, and downloads all files since 01/12/2019.
* Saves all messeges in a JSON.
* Saves a dict of file names and sent dates.


## The Files Parser
The object has 3 variables:
* path- file path
* _data- the parsed data
* \_output_dir - the dir to save the output in.


The parser is built in a structure of inheritance:

1. __Generic parser: FileParser.__ important Methods:
    1. run(): Runs the parser, and matches next level parser- by file type. 
    2. export_to_csv(): exports each table in the file to csv file. 
    3. parse_file(): empty method. 
2. __File-Type parser:__ PdfParser\ PptxParser. Uses specific modules to parse each file type. contains specific format parsers. important Methods:
    1. parse_file(): initial parse of the file, to detect spcific file structure. 
        1. The parse_file() of a File-type class needs to __append to self.\_data the parsed data.__
3. __Specific Structure parser:__ DailyUpdatePptxParser, CitiesPdfParser. Parses and rearange the data to readble format. important Methods:
    1. parse_file(): parses the file to the exact data format. 
        1. The parse_file() of a Specific Structure class needs to __return a LIST of the parsed tables.__
 