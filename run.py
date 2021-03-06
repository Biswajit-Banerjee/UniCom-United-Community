import pandas as pd
from tqdm import tqdm 
from datetime import datetime as dt
from pymongo import MongoClient
from flask import Flask
from flask import jsonify
import multiprocessing as mp 
import os
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.cluster.vq import whiten


# create flask app
app = Flask(__name__)


def replace_empty(x):
    """
    replace the empty value with unknown
    """
    if x.strip() == '':
        return 'Unknown'
    else:
        return x


def get_current_cov_state_data():
    """
    extract the current state wise COVID data 
    """
    # read raw data from below source and extract the required part
    raw_cov_data = pd.read_json(
            'https://api.rootnet.in/covid19-in/unofficial/covid19india.org'
    )
    cov_data = pd.DataFrame(raw_cov_data.data.rawPatientData)    
    cov_data = cov_data.loc[
            :, 
            [
                    'gender',
                    'city', 
                    'district', 
                    'state', 
                    'status'
            ]
    ]
    
    # replace status values to more proper value to show    
    cov_data.loc[:, 'status'] = (
            cov_data.loc[:, 'status'].str.replace('Hospitalized', 'Active'))
    
    # replace the missing values with unknow for gender    
    cov_data.loc[:, 'gender'] = cov_data.loc[:, 'gender'].apply(replace_empty)
    
    # get dummies for columns status and gender
    cov_data = pd.get_dummies(cov_data, columns=['status', 'gender'])
    
    # rename the columns to more accurate and ready to show names
    cov_data.rename(
            columns={"status_Active": "Active", 
                     "status_Deceased": "Deceased", 
                     "status_Migrated": "Migrated", 
                     "status_Recovered": "Recovered",
                     "gender_male": "Male", 
                     "gender_female": "Female", 
                     "gender_unknown": "Gender Unknow"}, 
             inplace=True
    )
    
    # create columns to calculate total active cases state wise
    cov_data.loc[:, 'Total'] = 1
    
    # group the data for each state and sum all the attributes
    state_cov_data = cov_data.groupby(['state']).sum()
    
    return state_cov_data


def add_cov_state_data_to_db():
    """
    add the covid data to the already created mongodb data base
    """
    cov_data = get_curresnt_cov_state_data()
    
    # fetch db URI from preset env var
    db_key = os.environ["DB_KEY"]

    # add all the data to mongo db
    # create client
    client = MongoClient(db_key)
    
    # open the db
    db = client.test
    
    # create new collection for state data
    collection = db.StateData
    
    # add each of the states
    for state in tqdm(cov_data.index):
        # Create the data record to be added to the db
        record = dict(
                    zip(
                        cov_data.loc[state, :].index.tolist(), 
                        cov_data.loc[state, :].astype('int').tolist()
                    )
        )
        # modify state name if emepty
        state = state if state != "" else "Unknow"
        record["state"] = state 
        record["last_updated"] = dt.now()
        
        # insert record to db
        collection.insert_one(record)

    
def update_state_data():
    """
    update the data in the db with latest update
    """
    cov_data = get_current_cov_state_data()
    
    # fetch db URI from preset env var
    db_key = os.environ["DB_KEY"]

    # add all the data to mongo db
    # create client
    client = MongoClient(db_key)
    
    
    # open the db
    db = client.test
    
    # create new collection for state data
    collection = db.StateData
    
    # add each of the states
    for state in tqdm(cov_data.index):
                
        record = dict(
                    zip(
                        cov_data.loc[state, :].index.tolist(), 
                        cov_data.loc[state, :].astype('int').tolist()
                    )
        )
        # modify state name if emepty
        state = state if state != "" else "Unknow"
        record["state"] = state 
        
        # replace the old data with the new one
        collection.replace_one(collection.find_one({"state": state}), record)
        

def get_data_from_db():
    # fetch db URI from preset env var
    db_key = os.environ["DB_KEY"]

    # add all the data to mongo db
    # create client
    client = MongoClient(db_key)
    
    # open the db
    db = client.test
    
    # create new collection for state data
    collection = db.StateData
    
    # get all the documents
    cursor = collection.find({})
    all_data = []
    for document in tqdm(cursor):
        # Remove _id from the document
        data = {key: value for key, value in document.items() if key != '_id'}
    
        all_data.append(data)
    return jsonify({"result": all_data})  


@app.route("/last_updated")
def get_date():
    # read data from website
    raw_cov_data = pd.read_json(
            'https://api.rootnet.in/covid19-in/unofficial/covid19india.org'
    )
    # extract date
    date = raw_cov_data['lastRefreshed'].lastRefreshed.replace("T", " ").replace("Z", "")
    date = date[:date.rfind(".")]

    # parse the date
    actual_date = dt.strptime(date, '%Y-%m-%d %H:%M:%S')
    
    return jsonify({"date": actual_date})


@app.route("/")  
def read_and_update_data():
    
    # create seperate process to update the db
    update_proc = mp.Process(target=update_state_data)
    update_proc.start()

    # return results of the read operation
    return get_data_from_db()


@app.route("/state/<state_name>")
def get_state_data(state_name):
    # fetch db URI from preset env var
    db_key = os.environ["DB_KEY"]

    # add all the data to mongo db
    # create client
    client = MongoClient(db_key)
    
    # open the db
    db = client.test
    
    # create new collection for state data
    collection = db.StateData

    # fetch regex matched state name
    document = collection.find_one({"state": {"$regex" : f".*{state_name}.*"}})

    # convert that data to  a dict removing the id
    data = {key: value for key, value in document.items() if key != '_id'}

    return jsonify({"result": data})



def get_cov_data_with_pop():
    
    cov_data = get_current_cov_state_data().reset_index()

    cov_data = cov_data.loc[
            :, 
            [
                    "state",
                    "Active",
                    "Deceased",
                    "Migrated",
                    "Recovered",
                    "Total"

            ]
    ]

    # Get population data
    df_pop = pd.read_csv(
                            'https://raw.githubusercontent.com/nishusharma1608/'
                            + 'India-Census-2011-Analysis/master/india-districts'
                            + '-census-2011.csv').loc[
                                                        :, 
                                                        [
                                                        'State name', 
                                                        'Population'
                                                        ]
                                                    ]
    
    df_pop.loc[:, 'State name'] = df_pop.loc[:, 'State name'].str.title()
    df_pop.loc[:, 'State name'] = df_pop.loc[:, 'State name'].str.replace("Pondicherry", "Puducherry")

    state_wise_pop = df_pop.groupby(['State name']).sum()

    cov_data.loc[:, 'state'] = cov_data.loc[:, 'state'].str.title()

    cov_with_pop = pd.merge(
                                cov_data, 
                                state_wise_pop, 
                                left_on=['state'], 
                                right_on=['State name'], 
                                how='left')

    cov_with_pop.set_index('state', inplace=True)
    # custom setting some missing data
    cov_with_pop.loc['Delhi', 'Population'] = 16787941
    cov_with_pop.loc['Ladakh', 'Population'] = 133487
    cov_with_pop.loc['Odisha', 'Population'] = 41974218
    cov_with_pop.loc['Telangana', 'Population'] = 35193978

    cov_with_pop.loc[:, 'Infected potion'] = (cov_with_pop.loc[:, 'Total'] / cov_with_pop.loc[:, 'Population']) * 100

    return cov_with_pop


@app.route("/ML/spread_prob")
def get_spreading_probability():

    cov_data = get_cov_data_with_pop().reset_index().dropna()

    X = whiten(cov_data.drop(["state"], 1))

    Z = linkage(X, 'ward', 'euclidean')

    cov_data['class'] = fcluster(Z, 6, criterion='maxclust') 

    cov_data["spread_prob"] = (1 - cov_data['class'] / 7) * 100

    cov_data["spread_prob"] = cov_data["spread_prob"].round(2)

    cov_data.set_index("state", inplace=True)

    cov_data.sort_values(["spread_prob"], inplace=True)

    all_records = []

    # add each of the states
    for state in tqdm(cov_data.index):
        record = {"state": state}
        record["Alert Level"] = cov_data.loc[state, "class"].astype('float')
        record["Infected portion"] = cov_data.loc[state, "Infected potion"].round(6)
        record["Spreading Probability"] =  cov_data.loc[state, "spread_prob"]

        all_records.append(record)
    return jsonify({'result': all_records})

