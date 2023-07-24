import boto3
import pandas
import json
import time
import uuid
from boto3.docs.docstring import ResourceWaiterDocstring
from fuzzywuzzy import fuzz, process
import decimal
from . import db


dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('crazy')

# batch add elements to db - OUTDATED
def update_db(ocean, listID):

    # few relevant colummns
    ocean.columns = ocean.columns.str.lower()
    ocean = ocean[['domain', 'company']]

    as_json = json.loads(ocean.to_json(orient='records'),
                         parse_float=decimal.Decimal)

    additional_columns = ['cellular', 'track', 'smart', 'module', 'data', 'device', 'security',
                          'remote', 'mobile', 'global', 'network', 'monitor', 'meter',
                          'telematic', 'telematics', 'fleet', 'temperature', 'tracker', 'battery',
                          'wireless', 'alert', 'plug', 'play', 'waterproof', 'deploy',
                          'real-time', 'real time', 'logger', 'antenna', 'grid', 'cat 1', 'gsm',
                          'iot', 'lte', 'lte-m', '2g', '3g', '4g', 'sigfox', 'lora',
                          'connectivity', 'm2m', 'gpsr', 'sensor', 'sim', 'esim', 'lpwan',
                          'bandwidth', 'embedded', 'nb-iot', 'nb', 'narrowband', 'cat-m1', '5g',
                          'gprs', 'cat m1', 'wifi', 'mesh', 'bluetooth', 'ble', 'unlicensed',
                          'lorawan', 'wi-fi', 'ethernet', 'zigbee', 'm-bus', 'wirepas', 'z-wave',
                          'rfid', 'class', 'id', 'score', 'solutions', 'linkedin']

    from_db = db.get_all_specify_attributes(
        ['company', 'id', 'processed', 'Processed'])


    company_id_map = {}
    companies = []
    company_processed_map = {}

    for c in from_db:
        if ('company' in c and 'id' in c):
            company_id_map[c['company']] = c['id']
            companies.append(c['company'])

            if 'processed' in c:
                company_processed_map[c['company']] = c['processed']
            elif 'Processed' in c:
                company_processed_map[c['company']] = c['Processed']
            else:
                company_processed_map[c['company']] = False


    items_added = []
    duplicate_companies = []

    # modify dataframe
    for idx, el in enumerate(as_json):
        for col in additional_columns:
            el[col] = None

        el['score'] = -10000
        el['processed'] = False
        el['exported'] = False
        el['export_ready'] = False
        el['updated_at'] = int(time.time())
        el['created_at'] = int(time.time())
        el['id'] = str(uuid.uuid4())
        el['list_id'] = listID

        known_company = process.extractOne(
            el['company'], companies, score_cutoff=95)

        if (known_company is not None):
            duplicate_companies.append(el['company'])
            print('Duplicate company: ', el['company'])

    # to_update = []
    to_db = []
    for item in as_json:
        try:
            if (item['domain'] is None):
                continue
            if (item['company'] in duplicate_companies):
                if not company_processed_map[item['company']]:  # add anyway!
                    print('Duplicated, but not processed yet', item['company'])
                    items_added.append(
                        {
                            'id':      company_id_map[item['company']],
                            'company': item['company'],
                            'domain':  item['domain'],
                        })


            else:

                to_db.append(item)
                print(item['company'])
                items_added.append(
                    {
                        'id':      item['id'],
                        'company': item['company'],
                        'domain':  item['domain'],


                    })
        except:
            print("Some random key error probably..")
            continue


    db.batch_add_elements(to_db)

    return items_added

# update company by company (msg by msg)
def update_db_single(company, company_list, company_id_map, company_processed_map):

    # columns to create
    additional_columns = ['cellular', 'track', 'smart', 'module', 'data', 'device', 'security',
                          'remote', 'mobile', 'global', 'network', 'monitor', 'meter',
                          'telematic', 'telematics', 'fleet', 'temperature', 'tracker', 'battery',
                          'wireless', 'alert', 'plug', 'play', 'waterproof', 'deploy',
                          'real-time', 'real time', 'logger', 'antenna', 'grid', 'cat 1', 'gsm',
                          'iot', 'lte', 'lte-m', '2g', '3g', '4g', 'sigfox', 'lora',
                          'connectivity', 'm2m', 'gpsr', 'sensor', 'sim', 'esim', 'lpwan',
                          'bandwidth', 'embedded', 'nb-iot', 'nb', 'narrowband', 'cat-m1', '5g',
                          'gprs', 'cat m1', 'wifi', 'mesh', 'bluetooth', 'ble', 'unlicensed',
                          'lorawan', 'wi-fi', 'ethernet', 'zigbee', 'm-bus', 'wirepas', 'z-wave',
                          'rfid', 'class', 'id', 'score', 'solutions', 'linkedin']


    items_added = []
    duplicate_companies = []

    el = company
    # modify dataframe
    for col in additional_columns:
        el[col] = None

    el['score'] = -10000
    el['processed'] = False
    el['exported'] = False
    el['export_ready'] = False
    el['updated_at'] = int(time.time())
    el['created_at'] = int(time.time())
    el['id'] = str(uuid.uuid4())
    # el['list_id'] = company

    known_company = process.extractOne(
        el['company'], company_list, score_cutoff=95) # check if other companies are similar (based on name similarity)

    print(len(company_list))
    print(company_list[0])

    if (known_company is not None):
        # company_list.append(el['company'])
        print('Duplicate company: ', el['company'])
        print(known_company)
        key = known_company[0]
        print("this is the key", key)
        known_company_id = company_id_map[key]
        print("matched company id: ", known_company_id)
        existing_company = db.getById(known_company_id)

        if (existing_company is None):
            print(
                "existing company is none. this should never ever happen. key error we guess")
            company_list.append(company['company'])

        elif (existing_company['processed'] is False):
            print("Add anywway, not yet processed")

            return {'company': el['company'], 'domain': el['domain'], 'id': known_company_id}
        else:
            return
    else:
        company_list.append(company['company'])

 

    to_db = []
    try:
        if (company['domain'] is None):
            return # domain is required for the company to be saved

        to_db.append(company) # add to db

        # update companuy id map
        company_id_map[company['company']] = company['id']
        print(company['company'] + ' added to db')

    except:
        print("Some random key error probably..")
        return


    try:
        db.batch_add_elements(to_db)
    except:
        print("db error")
        print(to_db)
    return to_db[0] if len(to_db) > 0 else None


if __name__ == "__main__":
    print('nono')
