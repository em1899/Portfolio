import json
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime
from datetime import date
from decimal import Decimal
from dateutil import tz
import traceback
import uuid
import requests as re

#parameters for slack reqests
url_slack = 'https://hooks.slack.com/services/id'
headers_slack = {
  "Content-type": "application/json"
}
#parameters for cognism request
headers_cog = {
    "Authorization": "congism_api_key",
    "Content-Type": "application/json"
    }  
    
#DynamoDB connection
client = boto3.client(
    'dynamodb',
    aws_access_key_id= "aws_access_key_id", 
    aws_secret_access_key= "aws_secret_access_key",
    region_name='eu-central-1'
)
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id= "aws_access_key_id", 
    aws_secret_access_key= "aws_secret_access_key",
    region_name='eu-central-1'
)

ddb_exceptions = client.exceptions
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('contact_enrichment')

#calculating number of matched requests for the given user
def get_request_number(index_name, prop, value):
    resp = dynamodb.Table('contact_enrichment').query(
        IndexName= index_name, 
        KeyConditionExpression=Key(prop).eq(value)
    )
    resp=resp['Items']
    today=date.today()

    date_req=[]
    no_match=0
    for row in resp:
        d=datetime.strptime(row['date'], '%Y-%m-%d %H:%M:%S.%f')
        date_req.append(d.date()) #total requests during the day
        try:
            if row['status']=='no match': #requests not matched
                no_match+=1
        except:
            continue
    req_number=date_req.count(today)
    return req_number-no_match #only matched requests are counted

#create entry in DynamoDB 
def create_entry(body):
    user=body['user_name'] #content from body of request
    command=body['command']
    text=body['text']
    id_=str(uuid.uuid4()) 
    date_=str(datetime.now())
    try:
        res=table.put_item(Item= {'id': id_, 'user':  user, 'date':date_, 'text': text }) #create entry
        print( {"statusCode":200, "res": res})
        return id_,text, user, command
    except Exception:
        print(traceback.format_exc())
        return {"statusCode":500, "err": traceback.format_exc()}

#modify attribute
def update_item(item_id, prop,value):
    try:
        res = dynamodb.Table('contact_enrichment').update_item(
            Key={'id': item_id},
            UpdateExpression="set #status = :status",
            ExpressionAttributeNames={
                "#status": prop,  #attribute to modify
            },
            ExpressionAttributeValues={
                ":status": value,
            }
        )
        return {"status":200, "res": res}
    except Exception:
        return {"status":500, "err": traceback.format_exc()}

#post request to cognism
def cognism_request(text):
        url_cog='https://api.cognism.com/api/prospector/people/match'

        data={'linkedin':text}
        response_cog = re.request("POST", url_cog, headers=headers_cog, data=json.dumps(data))
        return response_cog.json(), response_cog.status_code #return body and status code
  
 #cognism get request      
def get_data(cog_request): 
    id_=cog_request['id']
    url_cog='https://api.cognism.com/api/prospector/people/'+id_+'?api_key=key'
    payload={}
    
    response_cog = re.request("GET", url_cog, headers=headers_cog, data=payload)
    return response_cog.json()

    
def lambda_handler(event, context):
    body = json.loads(json.dumps(event, default=str))
    user=body['user_name']
    text=body['text']
    daily_request=get_request_number('user-index','user',user) 
    item_id,text,user,command=create_entry(body) #create entry in DynamoDB
    
    if daily_request>=5: #check if max daily requests has been hit 
        myobj = {'text': 'user '+user+' has reached the max request number for today'}
        slack = re.post(url_slack, json = myobj, headers=headers_slack)
        new_status=update_item(item_id,'status', 'max daily requests reached') #slack message

    else:
        labels=[]
        numbers=[]
        myobj = {'text': 'The request has been sent'}
        slack = re.post(url_slack, json = myobj, headers=headers_slack) #slack message
        cog_request, cog_status_code=cognism_request(text) #sending request 
        
        if cog_status_code!=200: #handling failed request
            add_status=update_item(item_id,'status', 'no match') 
            myobj = {'text': 'Unfortunately no phone numbers were found'}
            slack = re.post(url_slack, json = myobj, headers=headers_slack) #slack message

        else:
            #getting direct contacts
            if command=='/get_direct':
                name=cog_request['name']
                status={'name': name}
                number_dict={}
                print(cog_request['phone_numbers'])
                for phone in range(len(cog_request['phone_numbers'])):
                    labels.append(cog_request['phone_numbers'][phone]['label']) #list of phone labels
                if 'DIRECT_DIAL' in labels: #checking if direct phone label is present
                    data_retrieved=get_data(cog_request)['phone_numbers']
                    for phone in range(len(data_retrieved)):
                        if data_retrieved[phone]['label']== 'DIRECT_DIAL':
                            numbers.append(data_retrieved[phone]['number'])
                            number_dict.update({data_retrieved[phone]['number']:data_retrieved[phone]['label']}) #dictionary with phone number and label
                    status.update({'phone_numbers':number_dict}) #update db with results
                    text='Phone numbers for %s:\n'% (name) #buildig text for slack message
                    print(status)
                    for n in numbers: 
                        text+=n+' (DIRECT_DIAL),\n'
                    #text+='User %s has %d requests left for today' % (user, 5-daily_request-1) 
                    add_status=update_item(item_id,'status', status)
                
                    myobj2 = {'text': text}
                    slack2 = re.post(url_slack, json = myobj2, headers=headers_slack) #slack message
                    print(text)
                #handling no match for desired type
                else:
                    myobj3 = {'text': 'Unfortunately no phone numbers were found matching the desired type'}
                    slack2 = re.post(url_slack, json = myobj3, headers=headers_slack)
                    add_status=update_item(item_id,'status', 'no match') #updating db
                    
            #handling request for all phones
            elif command=='/get_phones':
                name=cog_request['name']
                status={'name': name}
                number_dict={}
                data_retrieved=get_data(cog_request)['phone_numbers'] #get data from cognism
                print(data_retrieved)
                if len(data_retrieved)>0:
                    for phone in range(len(data_retrieved)):
                        numbers.append(data_retrieved[phone]['number']+' '+data_retrieved[phone]['number_type'])
                        number_dict.update({data_retrieved[phone]['number']:data_retrieved[phone]['number_type']}) #create a dictionary with numbers and labels
                    status.update({'phone_numbers':number_dict})      
                    text='Phone numbers for %s:\n'% (name) #create text for slack message
                    print(status)
                    for n in numbers:
                        text+=n+' \n'
                    #text+='User %s has %d requests left for today' % (user, 5-daily_request-1)
                    add_status=update_item(item_id,'status', status) #update status in db
                
                    myobj2 = {'text': text}
                    slack2 = re.post(url_slack, json = myobj2, headers=headers_slack)
                else:
                    add_status=update_item(item_id,'status', 'no match')
                    text='No phone number were found'
                    myobj2 = {'text': text}
                    slack2 = re.post(url_slack, json = myobj2, headers=headers_slack) #slack message
            else:
                print('no')
                add_status=update_item(item_id,'status', 'no match')

    return {
        "daily request": daily_request,
        "response_type": text,
        "attachments": [
            {
                "text": 'yes' #response
            }
        ]
    }
