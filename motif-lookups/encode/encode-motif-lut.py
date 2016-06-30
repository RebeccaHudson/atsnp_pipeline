import re
import pickle


tf_map_file = 'encode-prefixes.txt' 
with open(tf_map_file) as f:
    lines = f.readlines()

lut_encode_prefixes = {} 

for line in lines:
    matchObj = line.split(" ") 
    prefix = matchObj[0]
    print "prefix " + prefix
    lut_encode_prefixes[prefix] = prefix 
    



def save_obj(obj, name ):
    with open(name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load_obj(name ):
    with open(name + '.pkl', 'rb') as f:
        return pickle.load(f)
  
save_obj(lut_encode_prefixes, 
         'lut_encode_prefixes')

happy = load_obj('lut_encode_prefixes')
print("happy? " + str(type(happy)))

