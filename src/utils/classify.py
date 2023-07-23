import logging
import pickle
import os
import pandas as pd

from utils import classifier
from .optimized import get_clf
import numpy as np
from sklearn.model_selection import cross_val_score
from dotenv import load_dotenv

load_dotenv()

classifier_storage_path = os.environ['CLASSIFIER_PATH']
classifier_order_storage_path = os.environ['CLASSIFIER_ORDER_PATH']

feature_mapping = {"telematics": "telematic", "real time": "real-time", "nb": "nb-iot",
                      "cat m1": "cat-m1", "cat 1": "cat-m1", "ble": "bluetooth", "lte-m": "cat-m1"}


feature_list = ['narrowband', 'sigfox', 'm2m', 'antenna',
        'temperature', 'embedded', '5g', 'rfid', 'data', 'security', 'module', 'sensor', 'cat-m1', 'connectivity', 'nb-iot',
        'lpwan', 'track', 'zigbee', 'monitor', 'global', 'logger',
        'mesh', 'updated_at', 'lora', 'sim', '2g', 'remote',
        'gpsr', 'wireless', 'lte', 'alert', 'device', 'cellular',
        'meter', 'plug', 'bandwidth', 'battery', 'network', 'wi-fi',
        'mobile', 'wifi', 'smart', '3g', 'gprs', 'tracker',
        'wirepas', 'unlicensed', 'gsm',
        'telematic', 'deploy', 'bluetooth', 'fleet', 'real-time', 'z-wave',
        'esim', 'iot', 'lorawan', 'ethernet', 'waterproof', '4g', 'm-bus',
        'play', 'grid']

class Classifier:
    def __init__(self):
        self.clf = self.get_classifier()       
        self.order = self.get_feature_order()

        print(self.clf)
        print(self.order)
 

    def check_init(self):
        return self.clf is not None and self.order is not None



    def get_classifier(self):
        print("Getting classifier")
        clf = None
        try:
            print("using existing classifier")
            with open(classifier_storage_path, 'rb') as f:
                clf = pickle.loads(f)
                print("floaded",clf)
        except:
            clf = None
        return clf
    
    def get_feature_order(self):
        print("Getting order")
        order = None
        try:
            with open(classifier_order_storage_path , 'rb') as f:
                order = pickle.loads(f)
        except:
            order = None
        return order


    def new_classifier(self, df):
        #heavy lifting.
        print("Training new classifier")

        try:
            with open(classifier_order_storage_path, 'wb') as f:
                pickle.dump(feature_list, f)
        except:
            logging.info("Couldn's store classifier.. ")
        self.order = feature_list
        # get a simple classifier going here.. 

        relevant_columns = feature_list + ['class']
        print("training set size:",len(df))
        df = df[relevant_columns]
        df.drop_na(inplace=True)
        print("training set size:",len(df))
        x = df[feature_list].values.astype(float)
        x[x>0] = 1

        y = df['class'].values.astype(float)


        clf = get_clf(x,y)

        self.clf = clf
        try:
            with open(classifier_storage_path, 'wb') as f:
                pickle.dump(clf, f)
        except:
            logging.info("Couldn's store classifier.. ")

        
        print("Cross val score: %f", np.mean(
            cross_val_score(clf, x, y, cv=20)))





    



    def exist(self, file):
        os.path.exists(classifier_storage_path+file)


    def classify(self, features):
        # features dict so we can guarentee order of features

        ordered_features =np.array([features[x] for x in self.order])

        # get correct format
        x = np.array(ordered_features)
        x.astype(float)
        x[x > 0] = 1
        x = x.reshape(1, -1)
       



        return self.clf.decision_function(x)


def combine_similar(df):

        df_f = df.copy()

        for col in df_f.columns:
            if col in feature_mapping:
                df_f[feature_mapping[col]] += df_f[col]
                df_f.drop([col], axis=1, inplace=True)

        return df_f

if __name__ == "__main__":
    with open('debug.pkl', 'rb') as f:
        from_db = pickle.load(f)
        df = pd.DataFrame(from_db)
        df_features = df[df['exported'] == True].copy()

        clf = Classifier()
        if not clf.check_init():
            clf.new_classifier(df)

        print(clf.clf)
