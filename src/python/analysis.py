# calculate cooccurrences that DO NOT ALREADY EXIST 

import util as u 
from datetime import datetime
import time 
import pymongo
from pymongo.errors import BulkWriteError 
import file_downloader as fd
import xml_to_mongo_db as db 
from itertools import combinations
import json 
import disk 
import os 

mesh_db,client   = db.get_db_client() 
errors = mesh_db.errors 
meshid = mesh_db.meshid 
pmid   = mesh_db.pmid 

log = print 

# -- 
#all_ids  = meshid.distinct('meshid')[0:1000]
#all_ids.sort() 
#mesh_pairs = combinations(all_ids,2)

with open("../../all_mesh_ids.json") as f : 
    mesh_data = json.load(f) 

def symptom_ids() : 
    # includes risk factors AND symptoms 
    rf = mesh_data['rf_ids'] 
    sx = mesh_data['symptom_ids'] 
    # - 
    rf_ids = [ x['meshID']['value'] for x in rf ] 
    sx_ids = [ x['meshID']['value'].split("/")[-1] for x in sx ] 
    # - return all of them 
    return rf_ids + sx_ids 
    
def disease_ids() : 
    return  [ x['meshID']['value'].split("/")[-1] for x in mesh_data['disease_ids'] ]
    
def get_parsed_ids() : 
    return { 'symptoms'  : symptom_ids() , 
             'diseases'  : disease_ids()   } 

def load_ids(ids) :  #list of mesh ids to retrieve 
    
    ids = ids   #[0:10]
    
    # --- 
    n   = len(ids) 
    # initialize the structure
    data = {} 
    for id in ids : 
        data[id] = [] 
    # -- 
    log("Loading data for {} ids...".format(n))
    # now populate it
    for i,id in enumerate(ids) : 
        log("{}/{}, ({}%)".format(i,n,100*i/n))
        # get all documents that match the id
        for doc in meshid.find({'meshid' : id},{'_id': False}) : 
            # and append them to the right spot in the structure 
            data[id].append(doc) 
    # - 
    log("Done") 
    return data 


def load_and_write(ids,parent_dir) :  #list of mesh ids to retrieve AND simultaneously write to disk
    
    ids = ids   #[0:10]
    
    # --- 
    n   = len(ids) 
    # -- create dir
    disk.mkdirp(parent_dir) 
    # -- 
    log("Loading data for {} ids...".format(n))

    for i,id in enumerate(ids) : 
        
        docs = []
        log("{}/{}, ({}%)".format(i,n,100*i/n))
        # get all documents that match the id
        for doc in meshid.find({'meshid' : id},{'_id': False}) : 
            # and append them
            docs.append(doc) 
            
        # now write it
        fname = os.path.join(parent_dir,id) 
        disk.dump_data(fname,docs)
        
    # - 
    log("Done") 


# - - - - - - - - - - - - -
def get_symptom_data() : 
    return load_ids(symptom_ids())

def get_disease_data() :
    return load_ids(disease_ids()) 

def load_and_write_disease_data(pdir) :
    load_and_write(disease_ids(),pdir) 



def dump_data(fname,data):
    log("Writing {}...".format(fname))
    with open(fname, 'w') as f:
        json.dump(data, f)
    log("Done") 

# - - - - - - - - - - - - - 
if __name__ == '__main__' : 
    go() 

    
    
