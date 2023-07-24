import json
import pandas as pd
import logging
import traceback
import boto3
import os
import sys
import requests as re

#initiate logging
sqs = boto3.client('sqs', region_name="eu-central-1")
queue_url = "https://sqs.eu-central-1.amazonaws.com/queuename"

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

# initate trello
url = "https://api.trello.com/1/cards"

headers = {
  "Accept": "application/json"
}
query = {
        'key': 'key',
        'token': 'token'
        }
#create a new card in the list
def move_card(list_name):
        index = list_name.rindex('/')
        query = {
        'idList': 'list_id',
        'key': 'key',
        'token': 'token',
        'name': list_name[index+1:],

    }
        response = re.request(
            "POST",
            url,
            headers=headers,
            params=query)
        return response


# return all cards in the "raw lists" column
def get_cards_in_raw():
    res = re.get(
    "https://api.trello.com/1/lists/list_id/cards",
    params=query
    ).json()
    cards={}
    for card in res:
        key= card['name']
        cards[key]=card['id']
    return cards

def delete_card(list_name):
    #match list name with the id of cards in that list
    cards= get_cards_in_raw() # get cards in raw list
    index = list_name.rindex('/')
    list_name=list_name[index+1:]
    if list_name in cards.keys(): 
        delete_id= cards[list_name] # delete card indicated as parameter
        print(delete_id)
        url = "https://api.trello.com/1/cards/"+delete_id
        query = {
        'key': 'key',
        'token': 'token'
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

    # based on the tool used (different naming conventions) use on of the following
    if argv[1]=='cognism':
        #standardize column names
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
                country=c['hq location'].split(',')[0] # get country if available
            except:
                country=''
            try:
                # send message to sqs
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
        #standardize column names
        for i in df.columns:
            if 'website' in i:
                df=df.rename(columns={"website":'domain'}, errors='raise')
        candidates = df.to_dict('records')
        print(candidates[0])
        list_id = os.path.basename(argv[0])
        for c in candidates:
            country=c['company country'] #for apollo only - get country
            try:
                # send message to sqs
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
    # update trello board by moving the list to the "raw lists" column (create + delete actions)
    move_card(argv[0]) 
    delete_card(argv[0])

if __name__ == "__main__":
    main(sys.argv[1:]) 