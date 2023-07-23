import utils.db as db
import sys
import pickle
import pandas as pd
import utils.classify as clf_util
import time
from datetime import datetime
from utils.doc import generate_doc
import traceback
import numpy as np
import decimal
import getopt

# load classifier

# get all non-processed data 

# 
relevant = ['narrowband', 'sigfox', 'm2m', 'antenna',
        'temperature', 'embedded', '5g', 'rfid', 'data', 'security', 'module', 'sensor', 'cat-m1', 'connectivity', 'nb-iot',
        'lpwan', 'track', 'zigbee', 'monitor', 'global', 'logger',
        'mesh', 'updated_at', 'lora', 'sim', '2g', 'remote',
        'gpsr', 'wireless', 'lte', 'alert', 'device', 'cellular',
        'meter', 'plug', 'bandwidth', 'battery', 'network', 'wi-fi',
        'mobile', 'wifi', 'smart', '3g', 'gprs', 'tracker',
        'wirepas', 'unlicensed', 'gsm',
        'telematic', 'deploy', 'bluetooth', 'fleet', 'real-time', 'z-wave',
        'esim', 'iot', 'lorawan', 'ethernet', 'waterproof', '4g', 'm-bus',
        'play', 'grid','class']


def generate(argv):

    export_path = ''
    default_list_len = 130
    export_specific_id = ''

    try:
        opts, args = getopt.getopt(argv, "hL:s:o:", ["ifile=", "ofile="])
    except getopt.GetoptError as err:
        print(err)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print("generate_list.py -L <listID> -s <companiesPerList> -o <exportPath>")
            sys.exit()
        elif opt == '-L':
            export_specific_id = arg

        elif opt == '-s':
            default_list_len = int(arg)
        elif opt == '-o':
            export_path = arg


    if export_path == '' :
        print("Export path required")
        sys.exit("Missing path... ")


    print("Export path: ", export_path)

    from_db = db.get_all()
  
    print("items in db", len(from_db))

    ready_for_export = [x for x in from_db if 'processed' in x and x['processed']]

    print(len(ready_for_export))

    # with open('debug.pkl', 'wb') as f:
    #     pickle.dump(from_db, f)

    # with open('/home/pb/debug.pkl', 'rb') as f:
    #     from_db = pickle.load(f)


    # print(from_db[0])
    # print(type(from_db[0]))

    df = pd.DataFrame(from_db)

    print(df['class'].unique())

    df = clf_util.combine_similar(df) # reduce similiar words to a single column.. 

    # print(len(df))
    # print(df['class'])

    # df_features = df[df['class'].isnull()].copy()
    df_features = df.copy()
    df_features = df_features[df_features['exported'] == False]


    unique_list_id_arr = df_features['list_id'].unique()
    print(unique_list_id_arr)
    #map from 
    unique_list_id = dict()
    idx = 0
    for id in unique_list_id_arr:
        if(id != 'noID'):
            unique_list_id[id] = idx
            idx +=1 

    print(unique_list_id_arr)
    if export_specific_id is not "":
        print('specific list...')
        print(export_specific_id)
        df_features = df.copy()

        df_features = df_features[df_features['list_id'] == export_specific_id]
        default_list_len = 1000



    print("ready for export..!: ",len(df_features))

    # df_features = df.copy()
    # df_features = df_features[df_features['updated_at'] >= 1638959417]

    df_training = df[~df['class'].isnull()].copy()
    df_training.reset_index(inplace=True) #new
    print("ready for export..!: ",len(df_features))
<<<<<<< HEAD
    print("ready for export..!: ",len(df_features))
=======
    #new bit
    for i in range(len(df_training)):           
        if df_training['class'][i]=='YES':
            df_training['class'][i]=1
        else:
            df_training['class'][i]=0
>>>>>>> 67e395d3f4111c9596cd534833b229728e78ddda
    # sys.exit()
    # print(df_training['class'].values.astype(float))
    df_training=df_training[relevant].dropna() #new


    clf = clf_util.Classifier()
    if not clf.check_init():
        print("init seems wrong..")
        clf.new_classifier(df_training)
    
    print("Training classes: ", df_training['class'].unique())

    as_dict = df_features.to_dict(orient='records')

    
    count = 0
    
    for company in as_dict:

        try:
            count += 1
            score = clf.classify(company)
            company['score'] = score[0]
            company['class']= score[1]
            print('class:'+ company['class'])
            company['export_ready'] = True
            company['exported'] = True
        except:
            print('Error in some company..')
            try:
                print(company['id'])
            except:
                continue
            # print(traceback.format_exc())





        # split the sorted list evenly across no_list lists....
    as_list = [el for el in as_dict if el['score'] is not None and type(el['company']) is str ]
    no_lists = int(round(len(as_list)/default_list_len) )

    print("Classified:", len(as_list))


    # sys.exit()

    if no_lists == 0 and len(as_list) > 0:
        no_lists = 1

    as_list.sort(reverse=True, key=lambda c: c['score'])
    split_companies = [[]] * no_lists  
    
    for i in range(no_lists):
        split_companies[i] = as_list[i::no_lists]

    # if(export_specific_id or export_all):
    #     for i in range(no_lists):
    #         split_companies[i] = as_list[i::no_lists]

    # else:
    #     for c in as_list:
    #         idx = unique_list_id[c['list_id']]
    #         split_companies[idx].append(c)
    
    
    # for ls in split_companies:
    #     print('len', len(ls), ls[0]['company'])


    print("Number of lists", no_lists)
    print('number of companies..', len(as_list))
    # print(split_companies)
    print('Generating documents')
  

    to_update = [] #only update db if export is success

    for file_idx in range(len(split_companies)):
            try:
                print(file_idx)
                print(split_companies[file_idx][0]['domain'])
                doc = generate_doc(split_companies[file_idx])

                name = export_path + 'leads' + datetime.now().strftime("%Y-%m-%d-%H-%M-%S") + \
                    '(' + str(file_idx) + ')' + '.pdf'
                # print(name)
                doc.output(name, 'F')
                print('Saved to: ' + name)
            except:
                print('could not save document')
                print(traceback.format_exc())

                continue

            try:
                companyNames = [c['company'] for c in split_companies[file_idx]]
                companyScore = [c['score'] for c in split_companies[file_idx]]
                companyDomains = [c['domain'] for c in split_companies[file_idx]]
                companyClass = [c['class'] for c in split_companies[file_idx]]
                companyId= [c['id'] for c in split_companies[file_idx]]
                listID= [c['list_id'] for c in split_companies[file_idx]]


                toLabel = pd.DataFrame()
                toLabel['Company'] = companyNames
                toLabel['Class'] = companyClass
                toLabel['Solution'] = ''
                toLabel['Domain'] = companyDomains
                toLabel['Score'] = companyScore
                toLabel['id'] = companyId
                toLabel['list_id'] = listID


                toLabel.to_csv(name.split('.')[-2] + '.csv')
                print("label file: " + name.split('.')[-2] + '.csv')

            except:
                print('Failed to generate the labeling file...')
                print(traceback.format_exc())
                continue
            
            
            tmp = []
            for i in range(len(companyId)):
                c = {}
                c['id'] = companyId[i]
                c['score'] = companyScore[i]
                tmp.append(c)


            to_update += tmp  
            # print("to update...:" ,to_update)



    # sys.exit()
    decimal.getcontext().prec = 5
    print('Updating DB')
    
    #really we should only do this for exported companies! TODO
    for company in to_update:
        try:    
            to_db = {}

            to_db['exported'] = True
            to_db['export_ready'] = True
            to_db['id'] = company['id']
            to_db['score'] = decimal.Decimal(str(company['score']))
            to_db['class']= company['class']
            to_db['updated_at'] = int(time.time())
            #print(company['id'])
            #print(to_db['id'])
            db.update(to_db)

        except:
            print('Failed for: ', to_db, company)
            print(traceback.format_exc())

            continue




    print("Done")
    sys.exit(0)





if __name__ == "__main__":
    generate(sys.argv[1:])



