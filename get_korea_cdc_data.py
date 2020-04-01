import os
import re
import timeit
import asyncio
import urllib.request
from datetime import datetime
import aiohttp

from bs4 import BeautifulSoup as BS

BASE_URL = "https://www.cdc.go.kr"


# TODO: this function should probably be an async generator.
async def get_all_report_links_(session, base_url):
    current_page = None
    report_links = []
    oldest_report_number = 'list_no=365797'

    async with session.get(f"{base_url}/board/board.es?mid=&bid=0030&nPage=1") as resp:
        html = await resp.text()
        current_page = BS(html, features='lxml')

    # TODO: this value will grow as more reports are added. Change this to dynamically
    # find the oldest relevant page.
    while True:
        current_page_report_links = [
            link.get('href') for link in current_page.find_all('a')]
        for report_link in current_page_report_links:
            if 'rss' in report_link:
                continue
            if 'list_no=' in report_link:
                report_links.append(report_link)

            # TODO: this condition will probably be a bug when this function will be truly async.
            if oldest_report_number in report_link:
                # reached the oldest relevant report (2020-01-22)
                return report_links

        next_page_link = current_page.find_all('a', class_='pageNext')[0].get('href')
        async with session.get(f'{base_url}{next_page_link}') as next_page_resp:
            next_page_html = await next_page_resp.text()
            current_page = BS(next_page_html, features='lxml')

    return report_links


def get_all_report_links(base_url):
    site = urllib.request.urlopen(f"{base_url}/board/board.es?mid=&bid=0030&nPage=1")
    site_soup = BS(site, features='lxml')
    report_links = []
    next_page = site_soup
    current_page = 0
    # TODO: this value will grow as more reports are added. Change this to dynamically find the oldest relevant page.
    while current_page != 15:
        print(f"gathered {len(report_links)}")
        links = [link.get('href') for link in next_page.find_all('a')]
        for l in links:
            if 'rss' in l:
                continue
            if 'list_no=' in l:
                report_links.append(l)
        next_page_link = next_page.find_all(
            'a', class_='pageNext')[0].get('href')
        next_page = BS(urllib.request.urlopen(
            f'{base_url}{next_page_link}'), features='lxml')
        current_page += 1
    # global glob_report_links = report_links


async def get_single_report(base_url, i, link, session, total_links_amount):
    print(f"({datetime.now()} - getting report {i+1} of {total_links_amount}: {link}")

    report_text = None
    async with session.get(f"{base_url}{link}") as resp:
        report_html = await resp.text()
        report_text = BS(report_html, features='lxml').get_text()

        report_date = re.search(r'Date\d{4}-\d{2}-\d{2}', report_text)
        date_filename = str(i) if not report_date else f"{report_date.group()[4:]}_{i}"

        if '2018' in date_filename or '2019' in date_filename:
            # A somewhat ugly way to detect when we went beyond the oldest relevant review
            return

        with open(os.path.join("reps", date_filename), "w+") as report_file:
            written = report_file.write(report_text)


async def main():
    print(f"starting to get all the linked reports ({datetime.now()})")

    async with aiohttp.ClientSession() as session:
        start_time = timeit.default_timer()

        glob_report_links = await get_all_report_links_(session, BASE_URL)
        total_links_amount = len(glob_report_links)
        coros = []
        for i, link in enumerate(glob_report_links):
            coros.append(get_single_report(BASE_URL, i, link, session, total_links_amount))
        print(len(coros))
        await asyncio.gather(*coros)

        end_time = timeit.default_timer()
        print(f"finished downloading {total_links_amount} which took {end_time - start_time} sec")


if __name__ == '__main__':
    asyncio.run(main())
