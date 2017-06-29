import re
import pickle

#This script produces a pickle file that will let you load a list of 
#ENCODE motifs from a dict given the prefix_* (transcription factor).

tf_map_file = 'encode-motifs.txt'



#lut_tfs_by_encode_motifs = {}
#For ENCODE transcription factor is the prefix before the first '_'.
def add_motif(to_dict, motif):
    motif_prefix = str.upper(motif.split('_')[0])
    if motif_prefix in to_dict:
        to_dict[motif_prefix].append(motif)
    else:
        to_dict[motif_prefix] = [motif]
    #to_dict is a dictionary,and is mutable, 
    #do I don't need to explicitly pass it back add forth.


#Given a transcription factor (prefix, here), deliver a list of motifs.
lut_encode_motifs_by_tf = {} 

with open(tf_map_file) as f:
    lines = f.readlines()

for line in lines:
    if line.find('>') == -1:
        continue
    motifs = line.split('>')[1:]
    motifs = motifs[0].split()

    for one_motif in motifs:
        print"adding motif " + one_motif
        add_motif(lut_encode_motifs_by_tf, one_motif)

def save_obj(obj, name ):
    with open(name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj(name ):
    with open(name + '.pkl', 'rb') as f:
        return pickle.load(f)
 
name_of_lut_pickle =  'lut_encode_motifs_by_tf'
save_obj(lut_encode_motifs_by_tf, name_of_lut_pickle)

check_for_working = load_obj(name_of_lut_pickle)
print("Did this load? " + str(type(check_for_working)))
