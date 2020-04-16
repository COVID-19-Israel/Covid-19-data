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
from datetime import datetime, date
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
    subtotal_suspected_col_idx = 6
    first_death_date = '2020-02-20'

    cols = data_row.find_all('td')
    raw_value = lambda val:val.get_text().replace(',', '').replace('.', '').replace('*','')

    test_data_row = {
        'date': -1,
        'total': -1,
        'confirmed' : -1,
        'recovered': -1,
        'isolated': -1,
        'deceased': -1,
        'being_tested': -1,
        'negative': -1,
    }
    cols_without_suspected_subtotal = cols

    test_data_row['date'] = report_date
    if date.fromisoformat(report_date) < date.fromisoformat(first_death_date):
        test_data_row['deceased'] = 0

    if len(cols) == 7:
        print(f'test row with 7 columns in {report_date}')
        return
    if len(cols) == 9:
        cols_without_suspected_subtotal = cols[:subtotal_suspected_col_idx] + cols[subtotal_suspected_col_idx+1:]
    if len(cols_without_suspected_subtotal) != 8:
        print(f'found a table with an unexpected number of columns. {len(cols_without_suspected_subtotal)}, {len(cols)}')
        return

    for i, key in enumerate(iter(test_data_row)):
        if test_data_row[key] != -1:
            continue
        test_data_row[key] = int(raw_value(cols_without_suspected_subtotal[i]))

    with open(os.path.join(BASE_OUTPUT_PATH, 'csv', f'{report_date}.csv'), 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=test_data_row.keys())
        writer.writeheader()
        writer.writerow(test_data_row)
        print(f'{test_data_row}')


def get_first_table_data(report_soup, report_date):
    table = report_soup.find('table')
    if table is not None:
        rows = table.find_all('tr')
        current_test_row = None
        if len(rows) == 3:
            current_test_row = rows[2]
        elif len(rows) == 5:
            current_test_row = rows[3]
        else:
            print(f"found a table with unexpected number of rows in {report_date}")
            return

        save_test_data_to_csv(current_test_row, report_date)


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
        queue.task_done()


async def get_reports(base_url, session, queue):
    i = 0

    while True:
        report_link = await queue.get()
        if report_link is None:
            continue
        asyncio.create_task(get_single_rep(base_url, report_link, session, queue, i))
        i += 1


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
