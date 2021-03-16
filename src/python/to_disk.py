import pathlib
import os 
import json




def dump_data(fname,data):
    with open(fname, 'w') as f:
        json.dump(data, f)
        
def load_data(fname) : 
    with open(fname, 'r') as f: 
        return json.load(f)

        
    
def mkdirp(p): 
    pathlib.Path(p).mkdir(parents=True, exist_ok=True)

# functions for exporting the loaded analysis information to disk 

def to_disk(parent_dir, data): 
    
    # make the top directory 
    mkdirp(parent_dir) 
    
    # get all keys (these will be files, where each file is a json object 
    ks = list(data.keys())
    n  = len(ks)
    
    for i,k in enumerate(ks) : 
        
        # monitor
        print("{}/{} - {}".format(i,n,k))
        
        # create fname 
        fname = os.path.join(parent_dir,k) 
        
        # get docs
        docs = data[k] 
        
        # write 
        dump_data(fname,docs) 
    
    
    
def from_disk(parent_dir) : 
    
    # get all keys (these will be files, where each file is a json object 
    ks = os.listdir(parent_dir)      
    n  = len(ks)
    
    # initialize data
    data = {} 
       
    for i,k in enumerate(ks) : 

        # monitor
        print("{}/{} - {}".format(i,n,k))

        # create full fname 
        fname = os.path.join(parent_dir,k)
    
        # get docs
        docs = load_data(fname) 
        
        # store
        data[k] = docs 
        
    # return
    return data 
    
