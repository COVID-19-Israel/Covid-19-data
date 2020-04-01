import os
import re
import timeit
import asyncio
import aiohttp
import urllib.request

from bs4 import BeautifulSoup as BS
from datetime import datetime

base_url = "https://www.cdc.go.kr"
glob_report_links = [
    "/board/board.es?mid=&bid=0030&act=view&list_no=366691&tag=&nPage=1",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366690&tag=&nPage=1",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366687&tag=&nPage=1",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366681&tag=&nPage=1",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366678&tag=&nPage=1",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366676&tag=&nPage=1",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366674&tag=&nPage=1",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366672&tag=&nPage=1",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366670&tag=&nPage=1",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366663&tag=&nPage=1",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366650&tag=&nPage=2",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366646&tag=&nPage=2",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366640&tag=&nPage=2",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366636&tag=&nPage=2",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366633&tag=&nPage=2",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366631&tag=&nPage=2",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366627&tag=&nPage=2",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366623&tag=&nPage=2",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366621&tag=&nPage=2",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366618&tag=&nPage=2",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366616&tag=&nPage=3",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366615&tag=&nPage=3",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366611&tag=&nPage=3",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366606&tag=&nPage=3",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366592&tag=&nPage=3",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366590&tag=&nPage=3",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366586&tag=&nPage=3",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366583&tag=&nPage=3",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366578&tag=&nPage=3",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366573&tag=&nPage=3",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366568&tag=&nPage=4",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366565&tag=&nPage=4",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366557&tag=&nPage=4",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366555&tag=&nPage=4",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366553&tag=&nPage=4",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366551&tag=&nPage=4",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366549&tag=&nPage=4",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366546&tag=&nPage=4",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366537&tag=&nPage=4",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366526&tag=&nPage=4",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366523&tag=&nPage=5",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366515&tag=&nPage=5",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366513&tag=&nPage=5",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366512&tag=&nPage=5",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366496&tag=&nPage=5",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366493&tag=&nPage=5",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366490&tag=&nPage=5",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366489&tag=&nPage=5",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366485&tag=&nPage=5",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366483&tag=&nPage=5",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366480&tag=&nPage=6",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366470&tag=&nPage=6",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366456&tag=&nPage=6",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366446&tag=&nPage=6",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366443&tag=&nPage=6",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366440&tag=&nPage=6",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366433&tag=&nPage=6",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366424&tag=&nPage=6",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366419&tag=&nPage=6",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366416&tag=&nPage=6",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366414&tag=&nPage=7",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366412&tag=&nPage=7",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366409&tag=&nPage=7",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366407&tag=&nPage=7",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366406&tag=&nPage=7",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366402&tag=&nPage=7",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366399&tag=&nPage=7",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366398&tag=&nPage=7",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366380&tag=&nPage=7",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366366&tag=&nPage=7",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366363&tag=&nPage=8",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366357&tag=&nPage=8",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366352&tag=&nPage=8",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366350&tag=&nPage=8",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366345&tag=&nPage=8",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366340&tag=&nPage=8",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366338&tag=&nPage=8",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366333&tag=&nPage=8",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366329&tag=&nPage=8",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366328&tag=&nPage=8",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366323&tag=&nPage=9",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366308&tag=&nPage=9",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366307&tag=&nPage=9",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366303&tag=&nPage=9",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366301&tag=&nPage=9",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366299&tag=&nPage=9",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366296&tag=&nPage=9",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366289&tag=&nPage=9",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366288&tag=&nPage=9",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366276&tag=&nPage=9",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366264&tag=&nPage=10",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366261&tag=&nPage=10",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366255&tag=&nPage=10",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366249&tag=&nPage=10",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366247&tag=&nPage=10",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366239&tag=&nPage=10",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366232&tag=&nPage=10",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366225&tag=&nPage=10",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366221&tag=&nPage=10",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366212&tag=&nPage=10",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366207&tag=&nPage=11",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366205&tag=&nPage=11",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366201&tag=&nPage=11",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366193&tag=&nPage=11",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366175&tag=&nPage=11",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366172&tag=&nPage=11",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366165&tag=&nPage=11",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366162&tag=&nPage=11",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366157&tag=&nPage=11",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366154&tag=&nPage=11",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366148&tag=&nPage=12",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366142&tag=&nPage=12",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366138&tag=&nPage=12",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366134&tag=&nPage=12",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366126&tag=&nPage=12",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366124&tag=&nPage=12",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366118&tag=&nPage=12",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366114&tag=&nPage=12",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366108&tag=&nPage=12",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366054&tag=&nPage=12",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366012&tag=&nPage=13",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366006&tag=&nPage=13",
    "/board/board.es?mid=&bid=0030&act=view&list_no=366003&tag=&nPage=13",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365999&tag=&nPage=13",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365993&tag=&nPage=13",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365987&tag=&nPage=13",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365981&tag=&nPage=13",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365953&tag=&nPage=13",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365941&tag=&nPage=13",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365933&tag=&nPage=13",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365932&tag=&nPage=14",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365926&tag=&nPage=14",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365915&tag=&nPage=14",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365911&tag=&nPage=14",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365901&tag=&nPage=14",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365893&tag=&nPage=14",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365888&tag=&nPage=14",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365879&tag=&nPage=14",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365875&tag=&nPage=14",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365872&tag=&nPage=14",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365869&tag=&nPage=15",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365867&tag=&nPage=15",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365849&tag=&nPage=15",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365844&tag=&nPage=15",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365830&tag=&nPage=15",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365797&tag=&nPage=15",
    "/board/board.es?mid=&bid=0030&act=view&list_no=365112&tag=&nPage=15",
    "/board/board.es?mid=&bid=0030&act=view&list_no=142926&tag=&nPage=15",
    "/board/board.es?mid=&bid=0030&act=view&list_no=142386&tag=&nPage=15",
    "/board/board.es?mid=&bid=0030&act=view&list_no=141989&tag=&nPage=15"]


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
        next_page_link = next_page.find_all('a', class_='pageNext')[0].get('href')
        next_page = BS(urllib.request.urlopen(f'{base_url}{next_page_link}'), features='lxml')
        current_page += 1


async def get_single_report(i, link, session):
    print(f"({datetime.now()} - getting report {i} of {len(glob_report_links)}: {link}")
    # report_html = urllib.request.urlopen(f"{base_url}{link}")

    report_text = None
    async with session.get(f"{base_url}{link}") as resp:
        report_html = await resp.text()
        report_text = BS(report_html, features='lxml').get_text()
    report_date = re.search(r'Date\d{4}-\d{2}-\d{2}', report_text)
    date_filename = str(i) if not report_date else report_date.group()[4:]

    if '2018' in date_filename or '2019' in date_filename:
        import ipdb; ipdb.set_trace()
        # A somewhat ugly way to detect when we went beyond the oldest relevant review
        return

    with open(os.path.join("reps", date_filename), "w+") as report_file:
        print(f"writing for {i}")
        report_file.write(report_text)
        await asyncio.sleep(0.3)


async def main():
    print(f"starting to get all the linked reports ({datetime.now()}")
    start_time = timeit.default_timer()

    async with aiohttp.ClientSession() as session:
        coros = []
        for i, link in enumerate(glob_report_links):
            coros.append(get_single_report(i, link, session))
        print(len(coros))
        await asyncio.gather(*coros)

    end_time = timeit.default_timer()
    print(f"finished downloading {len(glob_report_links)} which took {end_time - start_time} sec")


if __name__ == '__main__':
    asyncio.run(main())
