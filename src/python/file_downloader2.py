import util as u 
import os
import sys
import requests


log = u.register("file_downloader")

base_url = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
compressed_dir = os.path.join(os.getcwd(),"../../data/compressed/")

def get_file_base_name(n) : 
    num_str = "{:04d}".format(n)
    return base_url + "pubmed21n{}.xml.gz".format(num_str)

def get_all_base_names() : 
    start = 1 
    end   = 1062 # PARAMS! 
    return [ get_file_base_name(x) for x in range(start,end + 1 ) ] 

def url_from_base_name(n) :
    return base_url + n

def fname_from_base_name(n) :
    return compressed_dir + n 


## mehh - im changing the architecture here
def download_file(base) :
    url ,fname = url_from_base_name(base), fname_from_base_name(base)
    r = requests.get(url,stream=True)
    if r.status_code == 200:
        with open(fname, 'wb') as f:
            shutil.copyfileobj(r.raw,f)
    
