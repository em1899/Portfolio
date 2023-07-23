# automatic svm hyperparameter tuning using skopt for the ionosphere dataset

from sklearn.model_selection import cross_val_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC
from sklearn.model_selection import RepeatedStratifiedKFold
#from skopt import BayesSearchCV
import logging


def get_clf(X, y):



    logging.info("Optimizing classifier")
    # define search space
    params = dict()
    # params['C'] = (1e-6, 10.0, 'log-uniform')
    # params['gamma'] = (1e-6, 10.0, 'log-uniform')
    # params['degree'] = (1, 2)
    # params['kernel'] = ['linear', 'poly', 'rbf', 'sigmoid']

    # params['C'] = (1e-6, 10.0, 'log-uniform')
    # params['gamma'] = (9, 10.0, 'log-uniform')
    params['degree'] = (1,2)
    # params['kernel'] = ['linear']



    # # define evaluation
    # cv = RepeatedStratifiedKFold(n_splits=10, n_repeats=1, random_state=1)
    # # define the search
    # search = BayesSearchCV(
    #     estimator=SVC(class_weight='balanced', C=10, kernel='linear'), search_spaces=params, n_jobs=-1, cv=cv, verbose=1)
    # # perform the search

    # # print(X, y)
    #clf = SVC(class_weight='balanced', C=10, kernel='linear') RESTORE
    clf= RandomForestClassifier(criterion='entropy',random_state=0,criterion='entropy',random_state=0) #new

    clf.fit(X,y)
    return clf
    # search.fit(X, y)
    # report the best result
    print(search.best_params_)
    logging.info("Done optimizing classifier... ")
    return search.best_estimator_



