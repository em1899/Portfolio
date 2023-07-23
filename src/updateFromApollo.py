
from itertools import count
import utils.db as db
import sys
import pickle
import pandas as pd
import time
from datetime import datetime
import traceback
import numpy as np
from fuzzywuzzy import fuzz, process
from math import isnan

df = pd.read_pickle('db_updated.pkl')
companies = df['company'].to_list()

df = df.to_dict(orient='records')
# df -> to a map so we can look up ID of company

cmap = {}

for el in df:
    cmap[el['company']] = el


#  read from file when A. gets it...
dfApollo = pd.read_csv('~/crazy/db_july21.csv',
                       dtype={'Company Linkedin Url': object, 'Website' : object, 'Country' : object}).to_dict(orient='records')


# stats // maybe we match on the website instead?
matched_count = 0
unmatched_count = 0


idx = 0
# print(dfApollo)
for c in dfApollo:
    idx+=1

    print(idx)
    company = c['Company']
    # size = c['# Employees']
    # country = c['Company Country']
    # newDomain = c['Website']
    # newLinkedin = c['Company Linkedin Url']
    known_company = process.extractOne(
        company, companies, score_cutoff=95)

    # if(type(country) == float):
    #     country = None

    keywords = c['Keywords']
    if(type(keywords) == float):
        keywords = ''

    keywords = keywords.split(',') #list of keyworks instead.. !

    print(keywords)
    try:
        if(known_company is not None):
            matched_count += 1
            # located in the db
            # get id and update stuff
            el = cmap[company]
            # if(isnan(size)):
            #     size=None
            # else:
            #     size = int(size)
            # el['size'] = size
            # el['country'] = country


            # if 'domain' not in el:
            #     el['domain'] = ''
            #     print('fixed missing domain...')

            # if el['domain'] == 'noDomain.com' and type(newDomain) != float:
            #     el['domain'] = newDomain
            # else:
            #     el.pop('domain', None)

            # if el['linkedin'] == 'noLink.com' and type(newLinkedin) != float:
            #     el['linkedin'] = newLinkedin
            # else:
            #     el.pop('linkedin', None)

            # el.pop('company', None)
            # print(el)
            to_db = {}
            to_db['id'] = el['id']
            to_db['keywords'] = keywords
            db.update(to_db)

        else:
            unmatched_count += 1
    except:
        print('Failed for: ', c['Company'])

    print('Matched: ', matched_count, ' Unmatched: ', unmatched_count,
          'Rate: ', 100 * matched_count / (matched_count + unmatched_count))
