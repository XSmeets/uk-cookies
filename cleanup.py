import os
import re
import csv
import json
import pandas as pd

df = pd.read_csv('timestamps.csv')
df['File Path'] = df['File Path'].apply(lambda path: path.replace('/Users/xander/Git/collection-assistant/uk_cookies_data/', ''))

def extract_domain(value):
    result = re.search(r'([^\/]*)\/.*', value)
    if result is None:
        return None
    domain = result.group(1)
    
    # Filter fake 'domains', which were used for backup folder names (those folders have names containing a dash after the final dot, e.g. texas.gov-b).
    if re.search(r'.*\.[a-z]*-.*', domain) is not None:
        return None
    else:
        return domain

df['domain'] = df['File Path'].apply(extract_domain)

def extract_file_type(value):
    # First, let's see what kind of file we are dealing with
    result = re.search(r'[^\/]*\/.*\.([a-z\-]*)\.cookieblock\.json', value)
    if result is None:
        return None
    else:
        return result.group(1)

df['file type'] = df['File Path'].apply(extract_file_type)
df_indexed = df.dropna(subset=['file type']).set_index(['domain', 'file type'])
domains = pd.unique(df['domain'])
df_times = pd.DataFrame(list(domains), columns=['domain']).dropna()
def query_timestamp(row, file_type):
    try:
        # We add 1 second to the timestamp to account for the fact that modification timestamps are rounded down, whereas cookie timestamps (in Cookieblock) are given in milliseconds.
        return df_indexed.loc(axis=0)[row['domain'], file_type]['Modification Time'] + 1
    except KeyError:
        # print(row['domain'], file_type)
        return None
    except ValueError as e:
        # print(row['domain'], file_type)
        return None

df_times['accept'] = df_times.apply(query_timestamp, axis='columns', file_type='accept')
df_times['no-interaction'] = df_times.apply(query_timestamp, axis='columns', file_type='no-interaction')
df_times['no-interaction-first'] = df_times['no-interaction'] < df_times['accept']

df_times.set_index('domain', inplace=True)

def cookieblock_filter(cookieblock_data, phase_name, domain, cookie_set):
    cookieblock_dict = {(cookie_dict['name'], cookie_dict['domain'], cookie_dict['path']): [cookie['timestamp'] for cookie in cookie_dict['variable_data']] for paths in cookieblock_data.values() for cookies in paths.values() for cookie_dict in cookies.values()}

    filtered_cookies = []
    for cookie, timestamps in cookieblock_dict.items():
        if cookie not in cookie_set:
            cookie_set[cookie] = set(timestamps)
        else:
            # Take the existing timestamps from the dictionary; then, remove all timestamps which also occur in the current file.
            timestamps_filtered = [timestamp for timestamp in timestamps if timestamp not in cookie_set[cookie]]
            # Add the current set of timestamps to our dictionary.
            cookie_set[cookie] = cookie_set[cookie].union(set(timestamps))
            if len(timestamps_filtered) == 0:
                # We should not include this cookie in the filtered data
                continue
        # Add the cookie to the set of filtered cookies for this domain.
        filtered_cookies.append(cookie)

    with open(os.path.join(domain, f'{domain}.{phase_name}.cookieblock-filtered.json'), 'w') as outfile:
        json.dump(filtered_cookies, outfile)

with open('sites.tsv', 'r') as file:
    reader = csv.DictReader(file, delimiter='\t')

    cookies_total = {}
    for row in reader:
        domain = row['domain']

        cookieblock_accept = json.load(open(os.path.join(domain, f'{domain}.accept.cookieblock.json'), 'r'))
        cookieblock_no_interaction = json.load(open(os.path.join(domain, f'{domain}.no-interaction.cookieblock.json'), 'r'))
        cookieblock_reject = json.load(open(os.path.join(domain, f'{domain}.reject.cookieblock.json'), 'r'))
        cookieblock_withdraw = json.load(open(os.path.join(domain, f'{domain}.withdraw.cookieblock.json'), 'r'))
        
        if df_times.loc[domain, 'no-interaction-first']:
            cookieblock_filter(cookieblock_no_interaction, 'no-interaction', domain, cookie_set=cookies_total)
            cookieblock_filter(cookieblock_accept, 'accept', domain, cookie_set=cookies_total)
        else:
            cookieblock_filter(cookieblock_accept, 'accept', domain, cookie_set=cookies_total)
            cookieblock_filter(cookieblock_no_interaction, 'no-interaction', domain, cookie_set=cookies_total)
        cookieblock_filter(cookieblock_reject, 'reject', domain, cookie_set=cookies_total)
        cookieblock_filter(cookieblock_withdraw, 'withdraw', domain, cookie_set=cookies_total)