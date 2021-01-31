

import util as u
import medline_parser as mp
import os 

log = u.register("xml_parser")


# get files here 
compressed_dir = os.path.join(os.getcwd(),"../../data/compressed/")
xml_files = u.get_files_in_dir(compressed_dir) 

def parse_xml_file(fname) :
    dic = mp.parse_medline_xml(fname,
                               year_info_only=False,
                               nlm_category=False,
                               author_list=False,
                               reference_list=False) # return list of dictionary
    return dic


def to_mesh_json(single_xml) :

    to_extract = ['pmid' , 'country' , 'pubdate']
    common_dic = { k : single_xml[k] for k in to_extract } 

    return    [  dict({'meshid' : id}, **common_dic) for id in single_xml['mesh_terms'] ] 

