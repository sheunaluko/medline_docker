import util as u 
import os
import sys 

log = u.register("file_downloader")

base_url = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
compressed_dir = os.path.join(os.getcwd(),"../../data/compressed/")

def all_files() : 
    log.i("Generating files") 
    start = 1 
    end   = 1062 
    return [ get_file_name(x) for x in range(start,end + 1 ) ] 
    
def get_file_name(n) : 
    num_str = "{:04d}".format(n)
    return base_url + "pubmed21n{}.xml.gz".format(num_str) 

def get_last_n_files(n) : 
    log.i("Getting {} last filenames".format(n)) 
    files = all_files()
    files.reverse()
    return files[0:n] 

def download_file_set(fs) :
    num_skipped = 0
    fnames = [ f.replace(base_url, compressed_dir)  for f in fs ] 
    to_download = [f for f in fnames if not os.path.exists(f) ]
    log.i("Skipped {} files because they exist on disk already".format(len(fnames) - len(to_download)))
    
    for f in to_download : 
        os.system("curl -s {} --output {}".format(f.replace(compressed_dir,base_url),f)) # curl url, local disk name 
        log.i("Downloaded: {}".format(f))         

def download_last_n_files(n) :
    download_file_set(get_last_n_files(n)) 
