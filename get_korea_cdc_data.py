#!/usr/bin/env python3

"""
This crawler downloads the reports from the board of the Korean CDC (https://www.cdc.go.kr/board/board.es?mid=&bid=0030&nPage=1)
and saves text from all those reports as files which can then be searched for relevant data.
Install the dependencies before running:
pip install beautifulsoup4 aiohttp cchardet
"""

import os
import re
import timeit
import asyncio
from datetime import datetime
import aiohttp

from bs4 import BeautifulSoup as BS


BASE_URL = "https://www.cdc.go.kr"


async def get_all_report_links(session, base_url):
    # TODO: this function should probably be an async generator.

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

            # TODO: this condition will probably be a bug when this function is truly async.
            if oldest_report_number in report_link:
                # reached the oldest relevant report (2020-01-22)
                return report_links

        next_page_link = current_page.find_all('a', class_='pageNext')[0].get('href')
        async with session.get(f'{base_url}{next_page_link}') as next_page_resp:
            next_page_html = await next_page_resp.text()
            current_page = BS(next_page_html, features='lxml')

    return report_links


def save_report_to_file(path, filename, report):
    with open(os.path.join(path, filename), "w+") as report_file:
        report_file.write(report)


async def get_single_report(base_url, i, report_link, session, total_links_amount):
    print(f"({datetime.now()} - getting report {i+1} of {total_links_amount}: {report_link}")

    report_text = None
    async with session.get(f"{base_url}{report_link}") as resp:
        report_html = await resp.text()
        report_text = BS(report_html, features='lxml').get_text()

        report_date = re.search(r'Date\d{4}-\d{2}-\d{2}', report_text)
        date_filename = str(i) if not report_date else f"{report_date.group()[4:]}_{i}"
        save_report_to_file("Other_data/south-korea-cdc-reports", date_filename, report_text)


async def main():
    print(f"Getting all the reports, starting now. ({datetime.now()})")

    async with aiohttp.ClientSession() as session:
        start_time = timeit.default_timer()

        all_report_links = await get_all_report_links(session, BASE_URL)
        total_links_amount = len(all_report_links)

        coros = []
        for i, link in enumerate(all_report_links):
            coros.append(get_single_report(BASE_URL, i, link, session, total_links_amount))
        await asyncio.gather(*coros)

        end_time = timeit.default_timer()
        print(f"finished downloading {total_links_amount} reports which took {end_time - start_time} sec")


if __name__ == '__main__':
    asyncio.run(main())
