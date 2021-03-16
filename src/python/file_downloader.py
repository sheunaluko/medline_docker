import util as u 
import os
import sys
import subprocess 
#import requests
import shutil
import urllib.request as request
from contextlib import closing

log = u.register("file_downloader")

base_url = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
compressed_dir = os.path.join(os.getcwd(),"../../data/compressed/")
md5_dir = os.path.join(os.getcwd(),"../../data/md5/")

def get_file_base_name(n) : 
    num_str = "{:04d}".format(n)
    return  "pubmed21n{}.xml.gz".format(num_str)

def get_all_base_names() : 
    start = 1 
    end   = 1062 # PARAMS! 
    return [ get_file_base_name(x) for x in range(start,end + 1 ) ] 


base_names = get_all_base_names()

def url_from_base_name(n) :
    return base_url + n

def fname_from_base_name(n) :
    return compressed_dir + n

def expand_names(bn) :
    return url_from_base_name(bn) , fname_from_base_name(bn) 

    

## mehh - im changing the architecture here
# def download_file(base) :
#     url ,fname = url_from_base_name(base), fname_from_base_name(base)
#     r = requests.get(url,stream=True)
#     if r.status_code == 200:
#         with open(fname, 'wb') as f:
#             shutil.copyfileobj(r.raw,f)

def download_file(base) :
    url ,fname = expand_names(base) 
    return u.shell_output(["curl", url, "--output" , fname ])

def download_md5(base) :
    pass
    return u.shell_output(["curl", url, "--output" , md5_name ])

