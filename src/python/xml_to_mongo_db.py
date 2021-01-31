# utils for transfering a given xml file to mongo db 

import util as u 
import xml_parser as xmlp
from datetime import datetime
import pymongo
from pymongo.errors import BulkWriteError 

log = u.register("xml_to_mongo_db") 

# default config 
port = 27018
host = "localhost"
db_name = "meshdb" 

# get connection to mongo
def get_db_client() :
    # will need separate instance for each thread/process 
    client = pymongo.MongoClient("mongodb://{}:{}/".format(host,port))
    mesh_db = client[db_name]
    return mesh_db,client  

def export_file_to_db(fname,db,process_num=0): #will enable multiprocessing

    # helper logger function 
    def plog(msg) :
        log.i("[p{}]::{}".format(process_num,msg))
    
    # get start time 
    t_start = datetime.now()
    plog("Processing file: {}".format(fname))

    
    # parse file
    # TODO add logic for checking if there is a parsing error in the xml!
    # then after this can test simultaneous download and upload -- will be interesting
    # in fact could even simulate a half downloaded file ? 
    
    parsed = xmlp.parse_xml_file(fname)
    # get those with mesh terms 
    have_mesh = [x for x in parsed if x['mesh_terms'] != None]

    # prepare the mesh items to insert 
    mesh_to_insert = u.flatten_once( [xmlp.to_mesh_json(x) for x in have_mesh ] )

    # prepare the pmid items to insert
    # the transform done here is counting the number of mesh terms
    # AND referencing the fname it came from for debugging purpose
    pmid_to_insert = [ dict(x,mesh_num=len(x['mesh_terms']),fname=fname) for x in have_mesh ] 


    # and we write stuff now, catching any write errors
    error, error_msg = False, "" 
    try :
        
        pmids_written, mesh_written = None, None
        if (len(mesh_to_insert) < 1) :
            plog("No mesh terms to insert")
        else : 
            plog("Writing mesh...")
            mesh_result = db['meshid'].insert_many(mesh_to_insert)
            mesh_written = len(mesh_result.inserted_ids)         

        if (len(pmid_to_insert) < 1) :
            plog("No pmids to insert")
        else : 
            plog("Writing pmid...")
            pmid_result = db['pmid'].insert_many(pmid_to_insert)
            pmids_written  = len(pmid_result.inserted_ids)
                                 
        plog("Done")
                                 
    except BulkWriteError as bwe:
        plog("\n\nError with write :( -> {}".format(bwe.details))
        error, error_msg = True, bwe.details
        db.errors.insert_one({"fname" : fname, 't' : datetime.now() , 'error' : error_msg})        
    except Exception as e:
        plog("\n\nUnkown error with write --> for {}".format(fname))
        plog("Storing in db")         
        error, error_msg = True, str(e) 
        plog(error_msg)
        db.errors.insert_one({"fname" : fname, 't' : datetime.now() , 'error' : error_msg})

    t_end = datetime.now()

    if error :
        plog("\n\n")

    # prepare stats object for logging to db
    N,hm,fm = len(parsed), len(have_mesh), len(have_mesh)/len(parsed)

    info = {
        'num_articles' : N, 
        'num_with_mesh' : hm, 
        'fraction_with_mesh' : fm,
        'total_mesh_items_to_enter' : len(mesh_to_insert) ,
        'error' : error,
        'error_msg' : error_msg,
        't_start' : t_start, 
        't_end' : t_end ,
        'seconds_elapsed' : (t_end - t_start).total_seconds() , 
        'fname' : fname,
    }
    plog("Success={} | {} articles parsed, {}({}) have mesh and {} written. Mesh total written={}".format(not error,N,hm,fm,pmids_written,mesh_written))

    # save the info object as a log 
    plog("Writing log...")
    db['log'].insert_one(info)

    # and finally if there was no error and we finished we will update the "processed" collection
    if not error :
        db['processed'].insert_one({'fname' :fname})
        plog("No errors so updated progress collection with fname")

    plog("Finished with file: {}".format(fname))        
    
    
    
    
    
    
    
def already_processed(db) :
    ap = list(db.processed.find({}))
    return [ x['fname'] for x in ap ] 
