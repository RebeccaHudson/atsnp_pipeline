import re
import pickle
#This is where all of these transcription factors come from
#http://jaspar.binf.ku.dk/html/DOWNLOAD/ARCHIVE/JASPAR2010/JASPAR_CORE/pfms/pfms_all.txt


# then cat pfms_all.txt | grep ">" > jaspar_tf_map.txt 
#ended up using pfm_vertebrates_motif-lookups.txt

tf_map_file = 'pfm_vertebrates_motif-lookups.txt' 
with open(tf_map_file) as f:
    lines = f.readlines()

lut_jaspar_motifs_by_tf = {} 
lut_tfs_by_jaspar_motifs = {}

for line in lines:
    #matchObj = re.search(r'>(.+) (.+)+', line)
    matchObj = re.search(r'>(M[\w\.]+)', line)
    motif_value = matchObj.groups()[0]

    #tf = matchObj.groups(1)[1]
    tf = line.replace('>'+motif_value, "").strip()
    print "motif: " + motif_value + "     tf:" + tf

    lut_tfs_by_jaspar_motifs[motif_value] = tf
    
    # this one needs to be extended, because, even though the
    # mapping from motifs to transcription factors is one-to-one,
    # the mapping from transcription factors to motifs is,

    # in some cases, one-to-many.
    if lut_jaspar_motifs_by_tf.has_key(tf):
        if not type(lut_jaspar_motifs_by_tf[tf]) == list:
            lut_jaspar_motifs_by_tf[tf] = [lut_jaspar_motifs_by_tf[tf], motif_value]
        else: 
            lut_jaspar_motifs_by_tf[tf].append(motif_value)
    else:
         lut_jaspar_motifs_by_tf[tf] = motif_value
     



  #lookup works both ways!


def save_obj(obj, name ):
    with open(name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load_obj(name ):
    with open(name + '.pkl', 'rb') as f:
        return pickle.load(f)
  
save_obj(lut_jaspar_motifs_by_tf, 
         'lut_jaspar_motifs_by_tf')

save_obj(lut_tfs_by_jaspar_motifs,
         'lut_tfs_by_jaspar_motif')



happy = load_obj('lut_tfs_by_jaspar_motif')
print("happy? " + str(type(happy)))


# Need to rebuild this so that RUNX1 returns a list of 2 items...
slappy = load_obj('lut_jaspar_motifs_by_tf')
