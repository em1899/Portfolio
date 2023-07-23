import pandas as pd
import sys
from utils.add_companies_to_db import update_db
import logging
import traceback
import boto3
import uuid
import json
import getopt
import utils.db as db
import os
from dotenv import load_dotenv
load_dotenv()
import requests as re
print('hello')

sqs = boto3.client('sqs')
queue_url = os.environ['SQS_ENDPOINT_COMPANIES']

# logging.basicConfig(level=logging.DEBUG, filename='/home/ag/crazy.log', filemode="w+",
#                     format="Start new %(asctime)-15s %(levelname)-8s %(message)s")


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


def main(argv):
    try:
        opts, args = getopt.getopt(argv, "hi:", ["ifile="])
    except getopt.GetoptError:
        logging.info(
            "Missing argument - required: -i <inputfile> ")
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            logging.info("start_new_crazy.py <inputfile>")
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg

    print(inputfile)

    df = pd.read_csv(argv[0], delimiter=',', dtype={'domain': str})
    if len(df.columns)<2:
        df = pd.read_csv(argv[0], delimiter=';',  dtype={'domain': str})

    for i in df.columns:
        if 'website' in i:
            df=df.rename(columns={"website":'domain'}, errors='raise')
        if 'name' in i:
            df=df.rename(columns={"name":'company'}, errors='raise')

    #df=df[['company','domain']]
    candidates = df.to_dict('records')
    list_id = os.path.basename(inputfile)
    print("list_id", list_id)
    for c in candidates:
        try:

            put_sqs({'company': c['company'],
                     'domain': c['domain'],
                     'list_id': list_id})
            logging.info("Adding: %s to queue", c['company'])
            # print(id)
        except:
            logging.error(traceback.format_exc())
            print('error', c)


if __name__ == "__main__":
    main(sys.argv[1:])
