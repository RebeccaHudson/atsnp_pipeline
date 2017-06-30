import re
import pickle

#This script produces a pickle file that will let you load the linkable
#version of each ENCODE motif.

file_to_process  = 'encode4weblink.txt'

#For ENCODE transcription factor is the prefix before the first '_'.
def add_motif(to_dict, motif, motif_for_link):
    if not motif == motif_for_link:
        print " ".join(["adding", motif_for_link, "as link for", motif])
        to_dict[motif] = motif_for_link

#Given a transcription factor (prefix, here), deliver a list of motifs.
encode_motifs_for_link = {} 

with open(file_to_process) as f:
    lines = f.readlines()

for line in lines:
    motifs = line.split()
    motif = motifs[0]
    motif_for_link = motifs[1]
    add_motif(encode_motifs_for_link, motifs[0], motifs[1])

def save_obj(obj, name ):
    with open(name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj(name ):
    with open(name + '.pkl', 'rb') as f:
        return pickle.load(f)
 
name_of_lut_pickle =  'lut_encode_motifs_for_link'
save_obj(encode_motifs_for_link, name_of_lut_pickle)

check_for_working = load_obj(name_of_lut_pickle)
print("Did this load? " + str(type(check_for_working)))
