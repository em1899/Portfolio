from pandas.core.accessor import delegate_names
import boto3
import logging
import traceback
from multiprocessing.dummy import Pool  # use threads
from subprocess import check_output
import json
import os
import utils.db as db
from utils.custom_search.parser import parse
import time
# import simplejson
from dotenv import load_dotenv
from utils.add_companies_to_db import update_db_single
import pandas as pd

load_dotenv()
queue_url = os.environ['SQS_ENDPOINT_COMPANIES']
queue_url_scraper = os.environ['SQS_ENDPOINT']


sqs = boto3.client('sqs')

logging.basicConfig(level=logging.DEBUG, filename='/tmp/crazy.log', filemode="w+",
                    format="%(asctime)-15s %(levelname)-8s %(message)s")


def delete_message(receipt_handle):
    response = sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle,
    )


def put_sqs_to_scraper_queue(msg):
    response = sqs.send_message(
        QueueUrl=queue_url_scraper,
        DelaySeconds=0,
        MessageAttributes={
            'Type': {
                'DataType': 'String',
                'StringValue': 'companyID'
            }
        },
        MessageBody=json.dumps(msg))

    print(response['MessageId'])


def main():
    # pull queue forever

    try:
        db_cache = pd.read_csv('all.csv')
    except:
        db_cache = None

    print(type(db_cache), db_cache)

    if (db_cache is None):
        db_cache = db.get_all_specify_attributes(
            ['company', 'id', 'processed', 'Processed'])

    if (type(db_cache) == list):
        db_cache = pd.DataFrame(db_cache)
        # db_cache.to_csv('all.csv')
    # db_cache.to_pickle('db_cache.pkl')

    company_id_map = {}  # company name to id
    companies = []
    company_processed_map = {}
    db_cache = db_cache.to_dict('records')

    for c in db_cache:
        if ('company' in c and 'id' in c):

            try:
                company_id_map[c['company'].lower()] = c['id']
                companies.append(c['company'].lower())
            except:
                # print(c)
                continue
            if 'processed' in c:
                company_processed_map[c['company']] = c['processed']
            elif 'Processed' in c:
                company_processed_map[c['company']] = c['Processed']
            else:
                company_processed_map[c['company']] = False
        else:
            print("missing company or id", c)

    while (1):
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20
        )

        print("COMPANY SERVICE RUNNING")

        to_process = []
        receipt_handle = []
        try:
            messages = response.get("Messages", [])
            to_process = [json.loads(message['Body']) for message in messages]
            receipt_handle = [message['ReceiptHandle'] for message in messages]
        except:
            logging.error(traceback.format_exc())
            print(traceback.format_exc())

        try:
            for idx, el in enumerate(to_process):
                print(el)
                company_to_scrape = update_db_single(el, companies, company_id_map,
                                                     company_processed_map)

                print("company to scrape", company_to_scrape)
                if company_to_scrape is not None:
                    # add to scraper queue
                    put_sqs_to_scraper_queue(company_to_scrape)
            print("last element is now", companies[-1])

        except:
            print("major shiiit")
            print(traceback.format_exc())
            # print(company_id_map)
        for r in receipt_handle:
            delete_message(r)


if __name__ == "__main__":
    main()
