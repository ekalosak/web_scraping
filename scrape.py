import sys
import time
from lxml import html
import requests
import pandas as pd
import pdb
import logging

## Configure script
outnm = './huricane_warnings.csv'
retry_connections = 10

# Setup logging
log = logging.getLogger()
log.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)

# Get the webpage data
page_url = 'http://www.nhc.noaa.gov/archive/text/HSFEP3/'
page = requests.get(page_url)
tree = html.fromstring(page.content)
log.info(
        'Got page data and parsed into tree structure: {}'.format(page_url)
    )

# Scrape out the years of available reports
years_raw = tree.xpath('//a/text()')
years = [y for y in years_raw if y[0] == '2']
log.info('Extracted subdirectory names: {}'.format(years))

# Get the directory structure for each year page
df = pd.DataFrame(
        columns = ['year', 'filename', 'text']
    )
df.to_csv(outnm)

try:
    for yr in years:
        # construct the tree of the page
        sub_page_url = 'http://www.nhc.noaa.gov/archive/text/HSFEP3/' + yr
        log.debug('working on: {}'.format(sub_page_url))

        tries = 0
        while tries < retry_connections:
            try:
                page_yr = requests.get(sub_page_url)
                if page_yr.ok: break
            except requests.exceptions.ConnectionError as ee:
                log.error('Problems connecting to {}: {}'.format(
                    sub_page_url, ee))
                tries = tries + 1
        if tries == retry_connections:
            raise Exception('Couldnt connect, check log')

        tree_yr = html.fromstring(page_yr.content)
        log.debug('got page data and parsed into tree: {}'.format(sub_page_url))

        # get the names of the files in the year directory
        filenames_raw = tree_yr.xpath('//a/text()')
        filenames = [f for f in filenames_raw if f[0] == 'H']
        for fn in filenames:

            tries = 0
            while tries < retry_connections:
                try:
                    req = requests.get(sub_page_url + fn)
                    if req.ok: break
                except requests.exceptions.ConnectionError as ee:
                    log.error('Problems connecting to {}: {}'.format(
                        req.url, ee))
                    tries = tries + 1
                    time.sleep(5)
                    log.debug('trying again, tried {} times so far'.format(
                                tries))
            if tries == retry_connections:
                raise Exception('Couldnt connect, check log')

            row = yr[:-1], req.url, req.text
            df.loc[len(df)] = row
            log.debug('added row: {}'.format(row))

        log.info('Appending results to file: {}'.format(outnm))
        df.to_csv(outnm, mode = 'a', header = False)
        sys.exit(1)

except Exception as ee:
    log.error(ee)
    pdb.set_trace()

# log.info('Writing results to file: {}'.format(outnm))
# df.to_csv(outnm)
