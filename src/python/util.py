import glob 
import json 
import re 
import importlib 
import sys 
import math
import datetime 
import datetime 
import logging
from functools import reduce
import operator
import subprocess

#reloading stuff 
reload_children = set(["util"] ) 
def register(f) : 
    try :
        mod = sys.modules[f] 
    except KeyError : 
        return 
    
    reload_children.add(f) 
    def reloader() : 
        importlib.reload(mod) 
        print("Reloaded: " + f) 
    mod.r = reloader 
    return get_logger(f) # will return a logger object
    
def r() : 
    global reload_children 
    children = reload_children 
    for m in reload_children : 
        print("reloading: " + m)
        mod = sys.modules[m]
        importlib.reload(mod) 
    reload_children = children 


#logger 

# - 
logging.basicConfig(level=logging.DEBUG)

def get_logger(s) : 
    header = "[{}] ~ ".format(s) 
    
    def fn(x,t) : 
        if type(x) == str : 
            #simple string, will log it             
            l = getattr(logging,t) 
            l(header + x)
        else :
            #an object, will print the header first 
            l=getattr(logging,t)
            l(header)
            l(x) 
    
    class logger : 
        def __init__(self) : 
            pass 
        
        def i(self,x) : 
            fn(x,'info')
            
        def d(self,x) : 
            fn(x,'debug') 
            
        def e(self,x) : 
            fn(x,'error') 
            
    #return the new object 
    return logger()

log = get_logger('util')     
    


def flatten_once(l) :
    return reduce(operator.iconcat,l, [])
    
# params , referenc , etc ..

def check_for_file(fname)  : 
    import os.path
    return os.path.isfile(fname) 


def append_file(fname, strang) : 
    if not check_for_file(fname) : 
        mode = 'w' 
    else : 
        mode = 'a+' 

    with open(fname, mode) as outfile : 
        outfile.write(strang)

        # sub commands 
def sub_cmd(cmd,mode) : 
    import subprocess
    import sys
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    to_return = "" 
    if mode == "q" : 
        #do nothing 
        pass 
    else : 
        for c in iter(lambda: process.stdout.read(1), b''): 
            ch = c.decode()
            to_return += ch 
            if mode == "v" : 
                sys.stdout.write(ch)
    if mode == "s" : 
        return to_return 

def sub_cmd_v(cmd) : 
    return sub_cmd(cmd,"v")

def sub_cmd_q(cmd) : 
    return sub_cmd(cmd,"q")

def sub_cmd_s(cmd) : 
    return sub_cmd(cmd,"s")


def shell_output(cmd) :
    return subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE).communicate()

def contains(a1,a2) :  
    return bool(re.search(a2, a1))

def ensure_slash(d) : 
    #make sure there is trailing slash 
    if not d[-1] == "/" : 
        d = d + "/" 
    return d 


def get_files_in_dir(d) : 
    fs = glob.glob(ensure_slash(d) + "*" ) 
    fs.sort()
    return fs

# functional 
def map(f,l ) : 
    return  [ f(x) for x in l ] 

def extract_field_from_list(l,f) : 
    return [ x[f] for x in l ] 

def find_duplicates(coll) :   
    #tags: count, unique , ext 
    seen = {}
    dupes = []
    for x in coll:
        if x not in seen:
            seen[x] = 1
        else:
            if seen[x] == 1:
                dupes.append(x)
            seen[x] += 1
    return (seen,dupes) 

def partition(coll, group_size)  :
    return [coll[i:i+group_size] for i in range(0,len(coll),group_size) ]

def partition_in_order(coll, gs) :
    """will assign each succesive item in coll to a new partition"""
    parts = []
    for i in range(gs) :
        parts.append(coll[i::gs])
    return parts 
        




    


