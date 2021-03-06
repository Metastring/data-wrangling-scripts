#!/usr/bin/env python3

## Built over https://gist.github.com/vinayak-mehta/e5949f7c2410a0e12f25d3682dc9e873

import os
import sys
import requests
import re
import camelot
from lxml import etree
import pandas as pd
import roman

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


roman_numerals = [roman.toRoman(x).lower() for x in range(1, 70)]

def remove_roman_number(cell):
    split = cell.split()
    if len(split) > 0 and split[0].lower().rstrip('.') in roman_numerals:
        return ' '.join(split[1:])
    if len(split) > 0 and split[-1].lower().rstrip('.') in roman_numerals:
        return ' '.join(split[0:-1])
    split = cell.split('.')
    if len(split) > 0 and split[0].lower() in roman_numerals:
        return '.'.join(split[1:])
    else:
        return cell

def remove_leading_number(cell):
    split = cell.split()
    if len(split) > 0 and split[0].lower().rstrip('.').isnumeric():
        return ' '.join(split[1:])
    else:
        return cell

def isempty(text):
    return text.strip() == ""

def all_cells_empty(listofcells):
    return all(isempty(ele) for ele in listofcells)

def merge_overflowing_tables_to_previous_page(df):
    rowsToDelete = []
    for i, row in df.iterrows():
        if all_cells_empty([
            row['unique_id'], row['state'], row['district'],
            row['disease_illness'], row['num_cases'], row['num_deaths']
        ]):
            if not isempty(row['comment_action_taken']):
                rowsToDelete.append(i)
                merged = df.at[i - 1, 'comment_action_taken'] + " " + row['comment_action_taken']
                df.at[i - 1, 'comment_action_taken'] = merged
    df = df.drop(rowsToDelete)
    df = df.reset_index(drop=True)
    return df

def add_missing_district_from_above_row(df):
    for i, row in df.iterrows():
        if isempty(row['district']) and not isempty(row['disease_illness']):
            previous_row_district = df.at[i - 1, 'district']
            df.at[i, 'district'] = previous_row_district
    return df

def add_missing_state_from_above_row(df):
    for i, row in df.iterrows():
        if isempty(row['state']) and not isempty(row['district']):
            previous_row_state = df.at[i - 1, 'state']
            df.at[i, 'state'] = previous_row_state
    return df

def special_files(df, year, week):
    if year == 2012 and week == 3:
        for i, row in df.iterrows():
            if row['unique_id'].strip() != "":
                possible_split = row['unique_id'].split('. ')
                if len(possible_split) > 1 and row['state'].strip() == "":
                    df.at[i, 'state'] = possible_split[1]
                    df.at[i, 'unique_id'] = possible_split[0]
    return df

def clean_sheet(df, year, week):
    df = special_files(df, year, week)
    df = df.applymap(replace_extraneous_newlines)
    df = df.applymap(collapse_spaces)
    df['disease_illness'] = df['disease_illness'].apply(remove_roman_number)
    df['state'] = df['state'].apply(remove_leading_number)
    df = merge_overflowing_tables_to_previous_page(df)
    df = add_missing_district_from_above_row(df)
    df = add_missing_state_from_above_row(df)
    df = df.applymap(collapse_spaces)
    return df

def process_one_by_one(year = 2018, from_week=1, to_week=53, rewrite = False):
    for i in range(from_week, to_week + 1):
        csv_name = os.path.join(data_dir, str(year), '{}.csv'.format(i))
        if (not(os.path.exists(csv_name))):
            print(csv_name, " does not exist. Try running scrape.py first")
            continue
        df = pd.read_csv(csv_name, dtype=str, na_values=[], keep_default_na=False) # https://github.com/pandas-dev/pandas/issues/17810
        try:
            df = clean_sheet(df, year, i)
        except:
            print("ERROR at", csv_name)
            if (from_week == to_week):
                raise
        if (rewrite):
            filename = csv_name
        else:
            mkdir(os.path.join(data_dir, str(year), "clean"))
            filename = os.path.join(data_dir, str(year), "clean", '{}.csv'.format(i))
        df.to_csv(filename, index=False, quoting=1, encoding='utf-8')


directory = sys.argv[1] if len(sys.argv) > 1 else "2018"
from_week = sys.argv[2] if len(sys.argv) > 2 else "1"
to_week = sys.argv[3] if len(sys.argv) > 3 else "53"
process_one_by_one(int(directory), int(from_week), int(to_week), rewrite=False)