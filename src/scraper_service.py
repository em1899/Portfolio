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
import simplejson
from dotenv import load_dotenv


load_dotenv()
queue_url = os.environ['SQS_ENDPOINT']
config_path = os.environ['CRAZY_CONFIG']
data_path = os.environ['SCRAPER_DATA_PATH']
# /Applications/Screaming Frog SEO Spider.app/Contents/MacOS/ScreamingFrogSEOSpide$
cli_cmd = os.getenv('CLI_CMD', 'screamingfrogseospider')

sqs = boto3.client('sqs')

logging.basicConfig(level=logging.DEBUG, filename='/tmp/crazy.log', filemode="w+",
                    format="%(asctime)-15s %(levelname)-8s %(message)s")

p = Pool(1)  # should be environment variable?


def delete_message(receipt_handle):
    response = sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle,
    )


def crazy_instance(args):
    try:
        name = args['company']
        url = args['domain']
        path = data_path + '/' + args['id']
        if not os.path.exists(path):
            os.mkdir(path)
        return check_output([cli_cmd, '--headless', '--config', config_path, '--overwrite', '--export-tabs', 'Custom Search:All', '--output-folder',  path, '--crawl', url]), args['id'], None
    except Exception as e:
        logging.error(traceback.format_exc())
        print(traceback.format_exc())

        if (os.path.isfile(path + '/custom_search_all.csv')):
            print("already processed", path)
            return True, args['id'], None
        else:
            return None, args['id'], e


def main():
    # pull queue forever
    print(queue_url)
    while (1):
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20
        )

        print("I'm alive..")

        to_process = []
        receipt_handle = []
        try:
            messages = response.get("Messages", [])
            to_process = [json.loads(message['Body']) for message in messages]
            receipt_handle = [message['ReceiptHandle'] for message in messages]
        except:
            logging.error(traceback.format_exc())
            print(traceback.format_exc())

        for r in receipt_handle:
            delete_message(r)

        # do processing:
        args = []
        processed = []
        for c in to_process:
            args.append(c)
            print(c)
            logging.info("New company: %s, with id: %s", c['company'], c['id'])

        for output, id, error in p.imap_unordered(crazy_instance, args):
            if error:
                print(error)
                print("failed for ", id)
            if output:
                processed.append(id)
                print("Finished: ", id)

        # update database
        logging.info('updating database')
        for c in processed:
            # read the csv file and parse #probably do this differently...
            company, term_domain_map = parse(
                data_path + '/' + c + '/custom_search_all.csv')

            company['term_map'] = term_domain_map
            company['id'] = c
            company['processed'] = True
            company['updated_at'] = int(time.time())
            # company['exported'] = False
            # updates the difference
            try:
                db.update(company)
            except:
                # logging.info("Failed to update....: %s", company['id'])
                print(traceback.format_exc())
                try:
                    company.pop('term_map', None)
                    print("Error: ", company)
                except:
                    print("update error")


if __name__ == "__main__":
    main()
