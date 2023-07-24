import boto3
import logging
import traceback
from botocore.exceptions import ClientError
import decimal
import pandas as pd
dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('crazy')

# logging.basicConfig(level=logging.DEBUG, filename='/home/ag/crazy.log', filemode="w+",
#                     format="%(asctime)-15s %(levelname)-8s %(message)s")


def getById(id):
    # id = "94326ea6-776e-4c7d-a3d2-b84a6a6fe362"
    try:
        response = table.get_item(
            Key={'id': id})
        # print(response)
    except ClientError as e:
        logging.error(e.response['Error']['Message'])
        print(e)

    return response['Item'] if 'Item' in response else None


def getItemByKey(key, query):
    try:
        response = table.get_item(
            Key={key: query})
        print(response)
    except ClientError as e:
        logging.error(e.response['Error']['Message'])
        print(e)
    else:
        return response['Item']


def batch_add_elements(elements):

    try:
        with table.batch_writer() as batch:
            for item in elements:
                batch.put_item(Item=item)
    except:
        logging.error(traceback.format_exc())
        raise Exception("Failed to update DB")


def get_all_specify_attributes(attr):
    from_db = []
    done = False
    start_key = None
    scan_args = {'AttributesToGet': attr}
    # table.scan(AttributesToGet=['company'])['Items']

    while not done:
        if start_key:
            scan_args['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_args)
        items = response.get('Items', [])
        print("Retrieving: ", len(items))
        start_key = response.get('LastEvaluatedKey', None)
        from_db = from_db + items
        done = start_key is None
    df = pd.DataFrame(from_db).to_csv('/tmp/all.csv')
    return from_db


def get_all():
    from_db = []
    done = False
    start_key = None
    scan_args = {}
    # table.scan(AttributesToGet=['company'])['Items']
    print("Getting all items from DB")
    while not done:
        if start_key:
            scan_args['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_args)
        items = response.get('Items', [])
        print("Retrieving: ", len(items))
        start_key = response.get('LastEvaluatedKey', None)
        from_db = from_db + items
        done = start_key is None

    # parse for now..
    for el in from_db:
        if 'class' in el:
            if isinstance(el['class'], str):
                if el['class'].lower() == 'yes':
                    el['class'] = decimal.Decimal('1')
                elif el['class'].lower() == 'no'.lower():
                    el['class'] = decimal.Decimal('-1')

    return from_db


def update(element):  # note element contain unique ID used for identification.

    id = element['id']

    # no reason to update the id
    element.pop('id', None)

    attr, values, attr_names = get_update_params(element)

    # print(json.dumps(attr, indent=4))
    # print(json.dumps(values, indent=4))
    # print(json.dumps(attr_names, indent=4))

    try:
        response = table.update_item(
            Key={
                'id': id
            },
            UpdateExpression=attr,
            ExpressionAttributeValues=values,
            ExpressionAttributeNames=attr_names,
            ReturnValues="UPDATED_NEW",
        )
    except ClientError as e:
        logging.error(e.response['Error']['Message'])
        logging.error(traceback.format_exc())
        raise Exception('Failed to update element. ')

    else:
        return response


def get_update_params(body):
    """Given a dictionary we generate an update expression and a dict of values
    to update a dynamodb table.

    Params:
        body (dict): Parameters to use for formatting.

    Returns:
        update expression, dict of values.
    """
    update_expression = ["set "]
    update_values = dict()
    attribute_name = dict()

    for key, val in body.items():
        stripped_key = key.replace(' ', 'a').replace('-', 'b')

        update_expression.append(f" #{stripped_key} = :{stripped_key},")
        update_values[f":{stripped_key}"] = val

        attribute_name[f"#{stripped_key}"] = f"{key}"

    return "".join(update_expression)[:-1], update_values, attribute_name


if __name__ == "__main__":
    print("NOno. Don't call me directly!")
