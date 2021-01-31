# Wed 20 Jan 2021 03:33:05 PM CST
# Sheun Aluko

import util            as u 
import file_downloader as fd 
import xml_parser      as xmlp
import xml_to_mongo_db as db
import multi_xml_to_db as dbp
from datetime import datetime as dt 
import argparse

# arguments 
parser = argparse.ArgumentParser(description="Utilities for creating pubmed mesh metadata db")

parser.add_argument('--export-all',
                    dest='export_all',
                    action='store_true',
                    help="Parse and export all files in the datadir")

parser.add_argument('--cpus',
                    dest='ncpus',
                    type=int, 
                    default=dbp.default_cpus,
                    help="Number of CPUs to use for parsing and exporting")

parser.add_argument('--download-last',
                    dest='download_last_n',
                    type=int, 
                    help="Download the las N pubmed compressed xml archives directly from the ftp server")

args = parser.parse_args() 

if __name__ == "__main__" :

    log = u.get_logger("main")

    
    if (args.export_all) :

        if (args.ncpus > dbp.max_cpus ) :
            log.i("Cannot select more cpus than exist on the machine ({}). Please try again.".format(dbp.max_cpus))
            exit(1)

        log.i("Exporting all ({}) files to mongodb...".format(len(xmlp.xml_files)))
        start = dt.now()
        dbp.export_all_xmls_to_db_parallel(ncpus=args.ncpus)
        end = dt.now() 
        log.i("Done! Elapsed={}s".format( (end-start).total_seconds() ))
        
    elif (args.download_last_n) :
        log.i("Will download --> ")
        fd.download_last_n_files(args.download_last_n)
        
    else : 
        log.i("Nothing to do!") 

else :

    log = u.register("main") 


              
