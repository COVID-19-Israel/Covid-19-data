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
REPORT_DIR = "Other_data/south-korea-cdc-reports"

REPORT_QUEUE = None
DONE_FETCHING_LINKS_EVENT = None

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
    print(f"({datetime.now()} - getting report {i+1}: {report_link}")
    async with session.get(f"{base_url}{report_link}") as resp:
        report_html = await resp.text()
        report_text = BS(report_html, features='lxml').get_text()

        report_date = re.search(r'Date\d{4}-\d{2}-\d{2}', report_text)
        date_filename = str(i) if not report_date else f"{report_date.group()[4:]}_{i}"
        save_report_to_file(os.path.join(REPORT_DIR, 'text'), date_filename, report_text)
        save_report_to_file(os.path.join(REPORT_DIR, "html"), f"{date_filename}.html", report_html)
        # extract_statistic_data(report_html, report_date, date_filename)
        queue.task_done()


async def get_reports(base_url, session, queue):
    report_text = None
    i = 0

    while True:
        report_link = await queue.get()
        if report_link is None:
            continue
        asyncio.create_task(get_single_rep(base_url, report_link, session, queue, i))
        i += 1


async def get_single_report(base_url, i, report_link, session, total_links_amount):
    print(f"({datetime.now()} - getting report {i+1} of {total_links_amount}: {report_link}")

    report_text = None
    async with session.get(f"{base_url}{report_link}") as resp:
        report_html = await resp.text()
        report_text = BS(report_html, features='lxml').get_text()

        report_date = re.search(r'Date\d{4}-\d{2}-\d{2}', report_text)
        date_filename = str(i) if not report_date else f"{report_date.group()[4:]}_{i}"
        save_report_to_file(os.path.join(REPORT_DIR, 'text'), date_filename, report_text)
        extract_statistic_data(report_html, report_date, date_filename)

def save_test_data_to_csv(data_row: BS, report_date):
    TESTED_NOW_COL_INDEX = 2
    TESTED_NEGATIVE_COL_INDEX = 3
    cols = data_row.find_all('td')
    raw_value = lambda val: val.get_text().replace(',','').replace('.','')
    # tested_now = int(cols[len(cols)-TESTED_NOW_COL_INDEX].get_text().replace(',', '').replace('.', ''))
    tested_now = int(raw_value(cols[len(cols)-TESTED_NOW_COL_INDEX]))
    # tested_negative = int(cols[len(cols)-TESTED_NEGATIVE_COL_INDEX].get_text().replace(',', '').replace('.', ''))
    tested_negative = int(raw_value(cols[len(cols)-TESTED_NEGATIVE_COL_INDEX]))
    total_tested = tested_now + tested_negative

    with open(os.path.join(REPORT_DIR, 'csv', f'{report_date}.csv'), 'w', newline='') as csv_file:
        fieldnames = ['date', 'under_examination', 'negative', 'total_tested']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({'date': report_date, 'under_examination': tested_now,
                            'negative': tested_negative, 'total_tested': total_tested})
        print(f"tested now: {tested_now}, negative: {tested_negative}")


async def main2():

    tables = 0
    pages_scanned = 0
    # as_of_cols = 0
    unexpected_tables = 0

    scan_dir = os.path.join(REPORT_DIR, "html")
    # not bad. 160 pages had the table out of 176 pages.
    #
    filenames = os.listdir(scan_dir)
    filenames.sort()

    for report in filenames:
        report_date = report.replace('.html', '')
        pages_scanned += 1

        report_soup = None
        with open(os.path.join(scan_dir, report), 'r') as report_file:
            report_soup = BS(report_file.read(), features='lxml')
        
        # find out how many pages have the first table
        table = report_soup.find('table')
        if table is not None:
            tables += 1

            # find how many of those tables have the "As of X" row
            rows = table.find_all('tr')
            current_test_row = None
            if len(rows) == 3:
                current_test_row = rows[2]
            elif len(rows) == 5:
                current_test_row = rows[3]
            else:
                print(f"found a table with unexpected number of rows in {report}")
                unexpected_tables += 1
                continue

            save_test_data_to_csv(current_test_row, report_date)

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


async def main():
    print(f"Getting all the reports, starting now. ({datetime.now()})")
    REPORT_QUEUE = asyncio.Queue()
    REPORT_QUEUE.put_nowait(None)

    async with aiohttp.ClientSession() as session:
        start_time = timeit.default_timer()

        # all_report_links = await get_all_report_links(session, BASE_URL)
        # total_links_amount = len(all_report_links)


        coros = [
            asyncio.create_task(get_reports(BASE_URL, session, REPORT_QUEUE)),
            asyncio.create_task(get_all_report_links(session, BASE_URL, REPORT_QUEUE)),
            # get_reports(BASE_URL, session),
            # get_all_report_links(session, BASE_URL),
        ]

        # await DONE_FETCHING_LINKS_EVENT.wait()
        await REPORT_QUEUE.join()
        [coro.cancel() for coro in coros]
        # for i, link in enumerate(all_report_links):
        #     coros.append(get_single_report(BASE_URL, i, link, session, total_links_amount))
            # await get_single_report(BASE_URL, i, link, session, total_links_amount)
        await asyncio.gather(*coros, return_exceptions=True)

        end_time = timeit.default_timer()
        # print( f"finished downloading {total_links_amount} reports which took {end_time - start_time} sec")
        print( f"finished downloading. took {end_time - start_time} sec")


if __name__ == '__main__':
    asyncio.run(main())
