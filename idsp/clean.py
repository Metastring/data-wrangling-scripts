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


def mkdir(path):
    """Make directory
    Parameters
    ----------
    path : str
    """
    if not os.path.exists(path):
        os.makedirs(path)


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

def replace_extraneous_newlines(cell):
    cell = cell.replace('\n', ' ')
    return cell

def collapse_spaces(cell):
    return ' '.join(cell.split())

def remove_roman_number(cell):
    pass

def clean_sheet(df):
    df = df.applymap(replace_extraneous_newlines)
    df = df.applymap(collapse_spaces)
    return df

def process_one_by_one(year = 2018, rewrite = False):
    from_week = 1
    to_week = 53
    for i in range(from_week, to_week + 1):
        csv_name = os.path.join(data_dir, str(year), '{}.csv'.format(i))
        if (not(os.path.exists(csv_name))):
            print(csv_name, " does not exist. Try running scrape.py first")
            continue
        df = pd.read_csv(csv_name, dtype=str, na_values=[], keep_default_na=False) # https://github.com/pandas-dev/pandas/issues/17810
        df = clean_sheet(df)
        if (rewrite):
            filename = csv_name
        else:
            mkdir(os.path.join(data_dir, str(year), "clean"))
            filename = os.path.join(data_dir, str(year), "clean", '{}.csv'.format(i))
        df.to_csv(filename, index=False, quoting=1, encoding='utf-8')


directory = sys.argv[1] if len(sys.argv) > 1 else "2018"
process_one_by_one(directory)