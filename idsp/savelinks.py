#!/usr/bin/env python3

## Built over https://gist.github.com/vinayak-mehta/e5949f7c2410a0e12f25d3682dc9e873

import os
import sys
import requests
import re
from lxml import etree

data_dir = 'data'


def mkdir(path):
    """Make directory
    Parameters
    ----------
    path : str
    """
    if not os.path.exists(path):
        os.makedirs(path)


def scrape_web(year=2018, from_week=1, to_week=52):
    """Scrape PDFs from the IDSP website
    http://idsp.nic.in/index4.php?lang=1&level=0&linkid=406&lid=3689

    Parameters
    ----------
    year : int
    from_week : int
    to_week : int

    """
    year_dir = os.path.join(data_dir, str(year))
    mkdir(year_dir)

    crawl = {}

    r = requests.get('https://idsp.nic.in/index4.php?lang=1&level=0&linkid=406&lid=3689', verify=False)
    tree = etree.fromstring(r.content, etree.HTMLParser())
    table = tree.xpath('//*[@id="cmscontent"]')
    rows = table[0].cssselect('tr')
    for r in rows[1:]:
        try:
            y = int(r.cssselect('td')[0].cssselect('div')[0].cssselect('span')[0].cssselect('strong')[0].xpath('text()')[0])
        except IndexError:
            try:
                y = int(r.cssselect('td')[0].cssselect('span')[0].xpath('text()')[0])
            except IndexError:
                y = int(r.cssselect('td')[0].cssselect('div')[0].xpath('text()')[0])
        crawl[y] = {}
        links = r.cssselect('td')[1].cssselect('a')
        for l in links:
            try:
                m = re.search(r'\d+', l.xpath('text()')[0])
            except IndexError:
                m = re.search(r'\d+', l.cssselect('span')[0].xpath('text()')[0])
            week = int(m.group(0))
            link = l.xpath('@href')[0]
            crawl[y][week] = link
