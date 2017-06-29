import re
import pickle

#This script produces a pickle file that has keys and values
#That are each just one encode motif family prefix.
tf_map_file = 'encode-motifs-family.txt'

#lut_tfs_by_encode_motifs = {}
#For ENCODE transcription factor is the prefix before the first '_'.
def add_prefix(to_dict, family_prefix):
    if family_prefix in to_dict:
        return
    else:
        to_dict[family_prefix] = family_prefix
    #to_dict is a dictionary,and is mutable, 
    #do I don't need to explicitly pass it back add forth.

#Given a transcription factor (prefix, here), deliver a list of motifs.
lut_encode_motifs_by_tf = {} 

with open(tf_map_file) as f:
    lines = f.readlines()

longest_list = 0
for line in lines:
    line_parts = line.split()
    family_prefix = line_parts[-1]
    add_prefix(lut_encode_motifs_by_tf, family_prefix)

def save_obj(obj, name ):
    with open(name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj(name ):
    with open(name + '.pkl', 'rb') as f:
        return pickle.load(f)

print "completed processing, longest list : " + str(longest_list)
 
name_of_lut_pickle =  'encode_family_prefixes_only'
save_obj(lut_encode_motifs_by_tf, name_of_lut_pickle)

check_for_working = load_obj(name_of_lut_pickle)
print("Did this load? " + str(type(check_for_working)))
