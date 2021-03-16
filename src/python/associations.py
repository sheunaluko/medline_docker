import disk 
import os 





# helper 
def get_association_metrics(sset,l2) : 
    
    ps2 = [x['pmid'] for x in l2]
    common_pmids = list(sset.intersection(ps2)) #apparently ps2 does not need to be set here... 
    num_common = len(common_pmids)
    
    return { 'common_pmids' : common_pmids , 
             'num_common'   : num_common  } 


def write_counts(data, fname) : 
    
    ks = data.keys() 
    n  = len(ks)

    print("Collecting count information...") 
    
    counts = {}     
    
    for i,k in enumerate(ks) : 
        if (i % 400 == 0) : 
            print("Progress={}/{}".format(i,n))
        counts[k] = len(data[k])
        
    print("Writing {}".format(fname))    
    disk.dump_data(fname,counts)
    print("Done") 
    

# calculates association metrics and writes them to disk as it goes along 
# sdata/ddata generated by analysis.py (which can load them to mem or store to disk) 
def calc_associations(sdata,ddata,pdir="associations") : 
    
    sks = sdata.keys() 
    dks = ddata.keys() 
    
    sl,dl = len(sks), len(dks) 
    ncomp = sl*dl
    
    print("There are {} symptoms and {} diseases, which means {} comparisons".format(sl,dl,ncomp))
    
    disk.mkdirp(pdir)
    
    counter = 1
    nskipped = 0 
    nint = 0 
    
    for i,sk in enumerate(sks) :  
        
        # create subdir 
        dirname = os.path.join(pdir,sk)
        disk.mkdirp(dirname)        
        
        print("On sk={}, ({}/{})".format(sk,i,sl))
        
        # precompute the symptom set 
        symptoms = sdata[sk]
        symptom_set  = set( [s['pmid'] for s in symptoms] )
        
        # loop 
        for dk in dks : 
            
            # inc 
            counter = counter + 1 

            
            if (counter % 300 == 0 ) : 
                print("Sk={},Dk={},Skipd={},X={} | Total Progress={}/{} ({}%)".format(sk,dk,nskipped,nint,counter,ncomp,100*counter/ncomp))
                
            # I will store this on disk as associations/symptom_meshid/disease_meshid.json
            fname = os.path.join(dirname,dk+".json") 
            
            # check if the fname exists -- and if so just skip it! 
            if os.path.exists(fname) : 
                nskipped=nskipped + 1 
                continue 
                
            
            # we want to compute how many of the pmids are shared 
            # also will include the shared pmids -- why not 
            # get the association metrics now (see above fn) 
            data = get_association_metrics( symptom_set , ddata[dk] )
            
            # report intersections 
            if (data['num_common'] > 0 ) : 
                nint = nint + 1 
                disk.append_file("intersections", "{}_{}\n".format(sk,dk))
            
            # and write association data to disk! 
            disk.dump_data(fname,data)
            
            
            
    print("DONE!")
            
    
    
def get_associations(sk,pdir="associations") : 
    
    sdir = os.path.join(pdir,sk) 
    json_names = os.listdir(sdir) 
    
    for json_name in json_names : 
        fname = os.path.join(sdir,json_name)
        a_data = disk.load_data(fname)
        dk = json_name.replace(".json",'')
        
        
#D002908,Dk=D007859        

