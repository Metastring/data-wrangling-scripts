#!/usr/bin/env python3

## Built over https://gist.github.com/vinayak-mehta/e5949f7c2410a0e12f25d3682dc9e873

import os
import sys
import requests
import re
import camelot
from lxml import etree
import pandas as pd

data_dir = 'data'

## GLOBALS. Yes, a hack
crawl = None


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

    global crawl
    if (crawl is None):
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

    to_download = filter(lambda x: from_week <= x <= to_week, crawl[year])
    to_download = sorted(to_download)
    print('Found {} PDF(s) for download'.format(len(to_download)))
    if len(to_download):
        for w in to_download:
            print('Downloading year {}, week {} ...'.format(year, w))
            link = crawl[year][w]
            r = requests.get(link, stream=True, verify=False)
            filename = os.path.join(year_dir, '{}.pdf'.format(w))
            with open(filename, 'wb') as f:
                for chunk in r.iter_content(1024):
                    f.write(chunk)



def extract_tables(year=2018, from_week=1, to_week=52):
    """Extract tables from downloaded PDFs using Camelot
    Parameters
    ----------
    year : int
    from_week : int
    to_week : int
    """
    year_dir = os.path.join(data_dir, str(year))
    if not os.path.exists(year_dir):
        print('Year {} not found'.format(year))
        return None

    all_tables = []
    for i in range(from_week, to_week + 1):
        filename = '{}.pdf'.format(i)
        filename = os.path.join(year_dir, filename)
        print('Processing {} ...'.format(filename))
        # tables = camelot.read_pdf(filename, pages='2-end', line_size_scaling=40)
        tables = camelot.read_pdf(filename, pages='2-end', flavor='lattice', line_scale=40)

        print('Found {} tables(s)'.format(tables.n))
        all_tables.append(tables)
    return all_tables




headers = [
    'unique_id',
    'state',
    'district',
    'disease_illness',
    'num_cases',
    'num_deaths',
    'date_of_start_of_outbreak',
    'date_of_reporting',
    'current_status',
    'comment_action_taken',
    'reported_late',
    'under_surveillance'
]
ten_headers = [
    'unique_id',
    'state',
    'district',
    'disease_illness',
    'num_cases',
    'num_deaths',
    'date_of_start_of_outbreak',
    'date_of_reporting',
    'current_status',
    'comment_action_taken'
]
nine_headers = [
    'unique_id',
    'state',
    'district',
    'disease_illness',
    'num_cases',
    'num_deaths',
    'date_of_start_of_outbreak',
    'current_status',
    'comment_action_taken'
]


def append_tables(all_tables):
    """Append all tables in PDFs

    Parameters
    ----------
    all_tables : list

    """
    df = pd.DataFrame(columns=headers)
    for tables in all_tables:
        for table in tables:
            columns = list(table.df.iloc[0])
            if table.shape[1] == 10:
                temp = table.df.copy()
                if 'unique' in columns[0].lower():
                    temp = temp.iloc[1:]
                temp.columns = ten_headers
                temp['reported_late'] = False
                temp['under_surveillance'] = False
                df = pd.concat([df, temp], sort=False)
            elif table.shape[1] == 9:
                temp = table.df.copy()
                if 'disease' in columns[0].lower():
                    c = temp.iloc[0]
                    temp = temp.iloc[2:]
                    temp.columns = nine_headers
                    if 'reportedlate' in c[0].lower().replace(' ', ''):
                        temp['reported_late'] = True
                        temp['under_surveillance'] = False
                    elif 'undersurv' in c[0].lower().replace(' ', ''):
                        temp['reported_late'] = False
                        temp['under_surveillance'] = True
                    df = pd.concat([df, temp], sort=False)
                else:
                    temp.columns = nine_headers
                    temp['reported_late'] = True
                    temp['under_surveillance'] = False
                    df = pd.concat([df, temp], sort=False)
    return df

def process_one_by_one(year = 2018, from_week = 1, to_week = 53):
    for i in range(from_week, to_week + 1):
        pdf_name = os.path.join(data_dir, str(year), '{}.pdf'.format(i))
        if (not(os.path.exists(pdf_name))):
            scrape_web(year=year, from_week=i, to_week=i)
        try:
            all_tables = extract_tables(year=year, from_week=i, to_week=i)
            df = append_tables(all_tables)
            filename = os.path.join(data_dir, str(year), '{}.csv'.format(i))
            df.to_csv(filename, index=False, quoting=1, encoding='utf-8')
        except:
            print("FAILED TO READ FILE: ", year, i, "pdf")
        
directory = sys.argv[1] if len(sys.argv) > 1 else "2018"
from_week = sys.argv[2] if len(sys.argv) > 2 else "1"
to_week = sys.argv[3] if len(sys.argv) > 3 else "53"
process_one_by_one(int(directory), int(from_week), int(to_week))
