
import json
import boto3
import os
import pandas as pd
import time
import decimal
import uuid
dynamodb = boto3.resource('dynamodb')
table =  dynamodb.Table('crazy')



def migrate():
    #load all old shit and sort through it. Finally, add it all to the db
    data = read_db_local()
    data.columns = data.columns.str.lower()

    data['processed'] = True
    data['exported'] = True
    data['export_ready'] = True
    data['updated_at'] = int(time.time())
    data['id'] = str(uuid.uuid4())
    data['score'] = 0
    data['linkedin'] = 'noLink.com'
    data['solutions'] = ''
    data['domain'] = 'noDomain.com'

    as_json = json.loads(data.to_json(orient='records'), parse_float=decimal.Decimal)


    required_columns = ['cellular', 'track', 'smart', 'module', 'data', 'device', 'security',
    'remote', 'mobile', 'global', 'network', 'monitor', 'meter',
    'telematic', 'telematics', 'fleet', 'temperature', 'tracker', 'battery',
    'wireless', 'alert', 'plug', 'play', 'waterproof', 'deploy',
    'real-time', 'real time', 'logger', 'antenna', 'grid', 'cat 1', 'gsm',
    'iot', 'lte', 'lte-m', '2g', '3g', '4g', 'sigfox', 'lora',
    'connectivity', 'm2m', 'gpsr', 'sensor', 'sim', 'esim', 'lpwan',
    'bandwidth', 'embedded', 'nb-iot', 'nb', 'narrowband', 'cat-m1', '5g',
    'gprs', 'cat m1', 'wifi', 'mesh', 'bluetooth', 'ble', 'unlicensed',
    'lorawan', 'wi-fi', 'ethernet', 'zigbee', 'm-bus', 'wirepas', 'z-wave',
    'rfid', 'class', 'id', 'linkedin', 'score', 'exported', 'solutions', 'domain']


    for col in required_columns:
        if col not in data.columns:
            print('Error, ', col)


    # print(str(as_json))

    with table.batch_writer() as batch:
        for item in as_json: 
            item['id'] = str(uuid.uuid4())
            batch.put_item(Item=item)



def read_db_local():

    search_map = {"telematics": "telematic", "real time": "real-time", "nb": "nb-iot","cat m1": "cat-m1", "cat 1": "cat-m1", "ble": "bluetooth", "lte-m": "cat-m1"}



    path = '/Users/pb/onomondo/leadgeneration/server_backup/ubuntu/PoC/data/labeled'
    labeled_data = [
        os.path.join(dp, f)
        for dp, dn, fn in os.walk(os.path.expanduser(path))
        for f in fn
        if "_" in f
    ]

    frames = []

    for f in labeled_data:
        try:
            df = pd.read_pickle(f)
            # append all with updated tag...
            frames.append(df)
        except:
            continue

    classifier_data = pd.concat(frames, axis=0, ignore_index=True)

    # for c in classifier_data.index:
    #     print(c)

    print("Size before", len(classifier_data))

    # ind = classifier_data[classifier_data['class'] < -5].index
    # for i in ind:
    #     print(i)

    # remove all where class is -100
    classifier_data.drop(
        classifier_data[classifier_data['class'] < -5].index, inplace=True)
    print("Size after", len(classifier_data))

    # should we look for dublicated values?
    print("dropping dublicates")
    classifier_data.drop_duplicates(inplace=True, subset=['company', 'class'])
    print("Size after", len(classifier_data))

    
    return classifier_data



if __name__ == "__main__":
    migrate()


