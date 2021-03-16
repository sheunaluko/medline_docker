import util as u 
from datetime import datetime
import time 
import pymongo
from pymongo.errors import BulkWriteError 
import file_downloader as fd
import xml_to_mongo_db as db 


mesh_db,client   = db.get_db_client() 
errors = mesh_db.errors 
meshid = mesh_db.meshid 
pmid   = mesh_db.pmid 

log = print 

def validate_errors() : 
    # we want to make sure that the errors reported 
    # reflect actual import failures 
    log("here are the errors:") 
    for error in errors.find({}) : 
        print(error)
        # get the number of documents with the corresponding basename 
        bn = error['base_name']        
        print("Checking for {}".format(bn))
        num = pmid.count_documents({ 'base_name' : bn})
        print("Found {} documents\n".format(num))

              
        
def sanity_check() : 
    # test the count functionality 
    log("SANITY") 
    base_names = [ "pubmed21n{}.xml.gz".format(x) for x in [ 
        "0001" , "0010", "0100", "0200" ,"0300" ,"0601", "1000", "1200" 
    ]]
    tests = [ {'base_name' : x} for x in base_names ]
    for error in tests  : 
        print(error)
        # get the number of documents with the corresponding basename 
        bn = error['base_name']        
        print("Checking for {}".format(bn))
        num = pmid.count_documents({ 'base_name' : bn})
        print("Found {} documents".format(num))
              

if __name__ == '__main__' : 
    validate_errors() 
    sanity_check() 
