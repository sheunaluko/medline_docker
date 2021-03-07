
import util as u
import xml_to_mongo_db as db
import xml_parser as xmlp 
import multiprocessing
import math
import file_downloader as fd

log = u.register("multi_xml_to_db")

max_cpus     = multiprocessing.cpu_count()
default_cpus = max_cpus -1 

def export_files_to_db(process_num,fnames) :
    # return if there are no files to process 
    if len(fnames) < 1 :
        log.i("No files for process={}".format(process_num))
        return
    
    meshdb,dbclient = db.get_db_client() # get the db handle here
    # note this will be running in its own process
    # so we will loop through the assigned files and process them in series
    # also note -- there is a delay parameter set inside the export_file_to_db function
    # which helps to rate limit network requests - can do more work to figure this out
    # problem: with 7 processes going the network ftp requests failed after sometime
    # solution: initially implemented delay but than set delay=0 and only ran one cpu process at a time
    # future solution: compress the downloaded archive into a docker container for distribution
    # -- allow multiprocess extraction of the archive and dump into db 
    for fname in fnames :
        db.export_file_to_db(fname,meshdb,process_num)

def export_files_to_db_parallel(tmp_fnames,ncpus=default_cpus) :

    log.i("Will use {} cpu cores".format(ncpus))

    tmp_db,tmp_client = db.get_db_client()# tmp instance for query 
    already_exported = set(db.already_processed(tmp_db))
    tmp_client.close() # close this instance for now 

    log.i("{} files have already been processed".format(len(already_exported)))

    fnames = [ f for f in tmp_fnames if f not in already_exported ]

    if (len(already_exported) > 0 ) : 
        log.i("Thus, exporting {} files instead of {}!".format(len(fnames),len(tmp_fnames)))

    if len(fnames) < 1 :
        log.i("Nothing to export... exiting.")
        return 
    
    # partition_sz    = math.ceil(len(fnames)/ncpus) # old way 
    # file_partitions = u.partition(fnames,partition_sz) # old
    file_partitions = u.partition_in_order(fnames,ncpus) # new way -- better 
    log.i("Created {} file partitions: {}".format(len(file_partitions), [len(x) for x in file_partitions]))

    # build the argument tuples i.e. [ (process_num, files), ... ] 
    arg_tuples = enumerate(file_partitions) 
    
    # OK here we go...
    with multiprocessing.Pool(processes=ncpus) as pool:
        results = pool.starmap(export_files_to_db, arg_tuples)
    

def export_all_xmls_to_db_parallel(ncpus=default_cpus) :
    export_files_to_db_parallel(fd.base_names,ncpus)

def export_n_xmls_to_db_parallel(n,ncpus=default_cpus) :
    export_files_to_db_parallel(fd.base_names[0:n],ncpus)
    
    
                                
    
    
