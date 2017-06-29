import pickle

fname = 'ic_stat.txt'
with open(fname) as f:
    lines = f.readlines()

#Purpose of this experiment:
#If I set up a list of all the 'very high' content motifs,
#append that to an existing query, and run it, what happens?
print " ".join(["about to read", str(len(lines)), "lines"])

motif_ic = {}
for line in lines[1:]:
    split_line = line.split()
    motif = split_line[0]
    label = split_line[-1] 
    motif_ic[motif] = label 
    
with open('ic_stats' + '.pkl', 'wb') as f:
    pickle.dump(motif_ic, f, pickle.HIGHEST_PROTOCOL)   




 



