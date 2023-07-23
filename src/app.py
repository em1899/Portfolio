import json
import pandas as pd
import logging
import traceback
import boto3
import os
import sys
import requests as re

sqs = boto3.client('sqs', region_name="eu-central-1")
queue_url = "https://sqs.eu-central-1.amazonaws.com/355018875760/new-companies"
def put_sqs(msg):
    response = sqs.send_message(
        QueueUrl=queue_url,
        DelaySeconds=0,
        MessageAttributes={
            'Type': {
                'DataType': 'String',
                'StringValue': 'companyID'
            }
        },
        MessageBody=json.dumps(msg))
    print(response['MessageId'])

url = "https://api.trello.com/1/cards"

headers = {
  "Accept": "application/json"
}

def move_card(list_name):
        index = list_name.rindex('/')
        query = {
        'idList': '63ea0e07989e37d3f2be45ba',
        'key': '6a1d1262e9de39b3c3eb708298fcc328',
        'token': 'ATTAbaed9505f73dc78511c892c9f8b88ce317e357c0477ba0e77af90ea7da9c3c385864A07B',
        'name': list_name[index+1:],

    }
        response = re.request(
            "POST",
            url,
            headers=headers,
            params=query)
        return response


query = {
        'key': '6a1d1262e9de39b3c3eb708298fcc328',
        'token': 'ATTAbaed9505f73dc78511c892c9f8b88ce317e357c0477ba0e77af90ea7da9c3c385864A07B'
        }
def get_cards_in_raw():
    res = re.get(
    "https://api.trello.com/1/lists/629871c603973b6553eb8561/cards",
    params=query
    ).json()
    cards={}
    for card in res:
        key= card['name']
        cards[key]=card['id']
    return cards

def delete_card(list_name):
    #match list name with the id of cards in that list
    cards= get_cards_in_raw()
    index = list_name.rindex('/')
    list_name=list_name[index+1:]
    if list_name in cards.keys(): 
        delete_id= cards[list_name]
        print(delete_id)
        url = "https://api.trello.com/1/cards/"+delete_id
        query = {
        'key': '6a1d1262e9de39b3c3eb708298fcc328',
        'token': 'ATTAbaed9505f73dc78511c892c9f8b88ce317e357c0477ba0e77af90ea7da9c3c385864A07B'
        }
        del_response = re.request(
        "DELETE",
        url,
        params=query
        )

        print(del_response.status_code, 'list has been deleted yay')
    else:
        print('list in not in \'Raw Lists\'column')    

####

def main(argv):
    
    df = pd.read_csv(argv[0], delimiter=',', dtype={'domain': str})
    if len(df.columns)<2:
        df = pd.read_csv(argv[0], delimiter=',',  dtype={'domain': str})
    df.columns = df.columns.str.lower()
    print(df.shape)
    print(df.columns)

    if argv[1]=='cognism':
        for i in df.columns:
            if 'website' in i:
                df=df.rename(columns={"website":'domain'}, errors='raise')
            if 'name' in i:
                df=df.rename(columns={"name":'company'}, errors='raise')
        candidates = df.to_dict('records')
        print(candidates[0])
        list_id = os.path.basename(argv[0])
        for c in candidates:
            try:
                country=c['hq location'].split(',')[0]
            except:
                country=''
            try:
                put_sqs({'company': c['company'],
                          'domain': c['domain'],
                          'country': country,
                            'list_id': list_id})
                logging.info("Adding: %s to queue", c['company'])
            except:
                logging.error(traceback.format_exc())
                print('error', c)
    
    elif argv[1]=='apollo':
        print('apollo')
        for i in df.columns:
            if 'website' in i:
                df=df.rename(columns={"website":'domain'}, errors='raise')
        candidates = df.to_dict('records')
        print(candidates[0])
        list_id = os.path.basename(argv[0])
        for c in candidates:
            country=c['company country'] #for apollo only
            try:
                put_sqs({'company': c['company'],
                          'domain': c['domain'],
                          'country': country,
                        'list_id': list_id})
                logging.info("Adding: %s to queue", c['company'])
            except:
                logging.error(traceback.format_exc())
                print('error', c)

    else:
        print('use a valid type of list')
    move_card(argv[0])
    delete_card(argv[0])

if __name__ == "__main__":
    main(sys.argv[1:]) 