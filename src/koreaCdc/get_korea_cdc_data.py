#!/usr/bin/env python3

"""
This crawler downloads the reports from the board of the Korean CDC (https://www.cdc.go.kr/board/board.es?mid=&bid=0030&nPage=1)
and saves text from all those reports as files which can then be searched for relevant data.
Install the dependencies before running:
pip install beautifulsoup4 aiohttp cchardet aiodns
"""

import os
import re
import timeit
import asyncio
import csv
from datetime import datetime
import aiohttp

from bs4 import BeautifulSoup as BS


BASE_URL = "https://www.cdc.go.kr"
BASE_OUTPUT_PATH = "data/other/south-korea-cdc-reports"


async def get_all_report_links(session, base_url, queue):
    current_page = None
    report_links = []
    oldest_report_number = 'list_no=365797'
    first_page_url = "/board/board.es?mid=&bid=0030&nPage=1"

    async with session.get(f"{base_url}{first_page_url}") as resp:
        html = await resp.text()
        current_page = BS(html, features='lxml')

    while True:
        current_page_report_links = [
            link.get('href') for link in current_page.find_all('a')]
        for report_link in current_page_report_links:
            if 'rss' in report_link:
                continue
            if 'list_no=' in report_link:
                report_links.append(report_link)
                print(f'adding a link:{report_link}')
                # TODO: benchmark asyncio.create_task(queue.put) vs queue.put_nowait vs this.
                # From quick testing a few times this option was the fastest but it may well be a discrepancy...
                await queue.put(report_link)

            # TODO: this condition will probably be a bug when this function is truly async.
            if oldest_report_number in report_link:
                queue.task_done()
                return report_links

        next_page_link = current_page.find_all('a', class_='pageNext')[0].get('href')
        async with session.get(f'{base_url}{next_page_link}') as next_page_resp:
            next_page_html = await next_page_resp.text()
            current_page = BS(next_page_html, features='lxml')

    queue.task_done()
    return report_links


def save_report_to_file(path, filename, report):
    with open(os.path.join(path, filename), "w+") as report_file:
        report_file.write(report)


def save_test_data_to_csv(data_row: BS, report_date):
    tested_col_idx_from_end = 2
    tested_negative_col_idx_from_end = 1
    cols = data_row.find_all('td')
    raw_value = lambda val:val.get_text().replace(',', '').replace('.', '')
    tested_now = int(raw_value(cols[len(cols)-tested_col_idx_from_end]))
    tested_negative = int(raw_value(cols[len(cols)-tested_negative_col_idx_from_end]))
    total_tested = tested_now + tested_negative

    with open(os.path.join(BASE_OUTPUT_PATH, 'csv', f'{report_date}.csv'), 'w', newline='') as csv_file:
        fieldnames = ['date', 'under_examination', 'negative', 'total_tested']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({'date': report_date, 'under_examination': tested_now,
                         'negative': tested_negative, 'total_tested': total_tested})
        print(f"tested now: {tested_now}, negative: {tested_negative}")


def get_first_table_data(report_soup, report_date):
    table = report_soup.find('table')
    if table is not None:

        # find how many of those tables have the "As of X" row
        rows = table.find_all('tr')
        current_test_row = None
        if len(rows) == 3:
            current_test_row = rows[2]
        elif len(rows) == 5:
            current_test_row = rows[3]
        else:
            # print(f"found a table with unexpected number of rows in {report}")
            print(f"found a table with unexpected number of rows in {report_date}")
            return
            # unexpected_tables += 1
            # continue

        save_test_data_to_csv(current_test_row, report_date)


def extract_statistic_data(report_html, report_date, date_filename):
    """
    Get the following data from the report:
        number of severe cases
        number of isolated people
        number of tests.
    and save the data to a csv for this date.
    """

    # if first table in page:
    # get the third row.
    # get the last and second to last column (being tested and negative) and sum the values.
    # the difference for today: (last and second to last columns summed up)
    # Not every document has that table and some documents dont have that table with 5 rows.

    pass


async def get_single_rep(base_url, report_link, session, queue, i):
    print(f'({datetime.now()} - getting report {i+1}: {report_link}')
    async with session.get(f"{base_url}{report_link}") as resp:
        report_html = await resp.text()
        report = BS(report_html, features='lxml')
        report_text = report.get_text()

        report_date = re.search(r'Date\d{4}-\d{2}-\d{2}', report_text)
        date_filename = str(i) if not report_date else f'{report_date.group()[4:]}_{i}'
        save_report_to_file(os.path.join(BASE_OUTPUT_PATH, 'text'), date_filename, report_text)
        save_report_to_file(os.path.join(BASE_OUTPUT_PATH, 'html'),
                            f'{date_filename}.html', report_html)
        get_first_table_data(report, date_filename[:10])
        # extract_statistic_data(report_html, report_date, date_filename)
        queue.task_done()


async def get_reports(base_url, session, queue):
    i = 0

    while True:
        report_link = await queue.get()
        if report_link is None:
            continue
        asyncio.create_task(get_single_rep(base_url, report_link, session, queue, i))
        i += 1


async def main2():

    tables = 0
    pages_scanned = 0
    # as_of_cols = 0
    unexpected_tables = 0

    scan_dir = os.path.join(BASE_OUTPUT_PATH, "html")
    filenames = os.listdir(scan_dir)
    filenames.sort()

    for report in filenames:
        report_date = report.replace('.html', '')
        pages_scanned += 1

        report_soup = None
        with open(os.path.join(scan_dir, report), 'r') as report_file:
            report_soup = BS(report_file.read(), features='lxml')

        # find out how many pages have the first table
        get_first_table_data(report_soup, report_date)

        # for row in table.find_all('tr'):
        #     first_col = row.find('td')
        #     if first_col is not None:
        #         first_col_text = first_col.get_text()
        #         if "as of" in first_col_text.lower():
        #             as_of_cols += 1

    print(f"scanned {pages_scanned} pages and found {tables} first tables.")
    print(
        f'out of those {tables} tables, {unexpected_tables} unexpected tables were found that were not parsed.')
    # print(f'found {as_of_cols} "as of" columns in {tables} tables')


def create_output_dirs():
    os.makedirs(os.path.join(BASE_OUTPUT_PATH, "text"), mode=0o755, exist_ok=True)
    os.makedirs(os.path.join(BASE_OUTPUT_PATH, "csv"), mode=0o755, exist_ok=True)
    os.makedirs(os.path.join(BASE_OUTPUT_PATH, "html"), mode=0o755, exist_ok=True)


async def main():
    print(f"Getting all the reports, starting now. ({datetime.now()})")
    report_queue = asyncio.Queue()
    report_queue.put_nowait(None)

    create_output_dirs()
    async with aiohttp.ClientSession() as session:
        start_time = timeit.default_timer()
        coros = [
            asyncio.create_task(get_reports(BASE_URL, session, report_queue)),
            asyncio.create_task(get_all_report_links(session, BASE_URL, report_queue)),
        ]

        await report_queue.join()
        [coro.cancel() for coro in coros]
        await asyncio.gather(*coros, return_exceptions=True)

        end_time = timeit.default_timer()
        print(f"finished downloading. took {end_time - start_time} sec")


if __name__ == '__main__':
    asyncio.run(main())
