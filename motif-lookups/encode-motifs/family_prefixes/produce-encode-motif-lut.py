import re
import pickle

#This script produces a pickle file that will let you load a list of 
#ENCODE motifs from a dict given the prefix_* (transcription factor).
tf_map_file = 'encode-motifs-family.txt'


#lut_tfs_by_encode_motifs = {}
#For ENCODE transcription factor is the prefix before the first '_'.
def add_motif(to_dict, motif, family_prefix, longest_list):
    if family_prefix in to_dict:
        to_dict[family_prefix].append(motif)
        if len(to_dict[family_prefix]) > longest_list:
            longest_list = len(to_dict[family_prefix]) 
    else:
        to_dict[family_prefix] = [motif]
    return longest_list
    #to_dict is a dictionary,and is mutable, 
    #do I don't need to explicitly pass it back add forth.


#Given a transcription factor (prefix, here), deliver a list of motifs.
lut_encode_motifs_by_tf = {} 

with open(tf_map_file) as f:
    lines = f.readlines()

longest_list = 0
for line in lines:
    line_parts = line.split()
    print "line parts " + repr(line_parts)
    family_prefix = line_parts[-1]
    motif = line_parts[0]
    longest_list = \
      add_motif(lut_encode_motifs_by_tf, motif,
                 family_prefix, longest_list)

def save_obj(obj, name ):
    with open(name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj(name ):
    with open(name + '.pkl', 'rb') as f:
        return pickle.load(f)

print "completed processing, longest list : " + str(longest_list)
 
name_of_lut_pickle =  'lut_encode_motifs_by_family'
save_obj(lut_encode_motifs_by_tf, name_of_lut_pickle)

check_for_working = load_obj(name_of_lut_pickle)
print("Did this load? " + str(type(check_for_working)))
