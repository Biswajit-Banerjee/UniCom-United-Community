import pandas as pd
from tqdm import tqdm 
from datetime import datetime as dt
from pymongo import MongoClient
from flask import Flask
from flask import jsonify
import multiprocessing as mp 


app = Flask(__name__)


def replace_empty(x):
    """
    replace the empty value with unknown
    """
    if x.strip() == '':
        return 'Unknown'
    else:
        return x

def get_curresnt_cov_state_data():
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
    
    # add all the data to mongo db
    # create client
    client = MongoClient(
            'mongodb+srv://dbuser:admindbuser890@unity-community-bcw3m.mongodb.net/test?retryWrites=true&w=majority'
    )
    
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
    cov_data = get_curresnt_cov_state_data()
    
    # create client
    client = MongoClient(
            'mongodb+srv://dbuser:admindbuser890@unity-community-bcw3m' 
            + '.mongodb.net/test?retryWrites=true&w=majority'
    )
    
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
        record["last_updated"] = dt.now()
        
        
        # replace the old data with the new one
        collection.replace_one(collection.find_one({"state": state}), record)
        

def get_data_from_db():
    # create client
    client = MongoClient(
            'mongodb+srv://dbuser:admindbuser890@unity-community-bcw3m' 
            + '.mongodb.net/test?retryWrites=true&w=majority'
    )
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


@app.route("/", methods=['GET'])  
def read_and_update_data():
    
    # create seperate process to update the db
    # update_proc = mp.Process(target=update_state_data)
    # update_proc.start()

    # return results of the read operation
    # return get_data_from_db()
    return "Hello World!"