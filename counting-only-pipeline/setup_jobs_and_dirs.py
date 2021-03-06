import os
import math
import shutil
import os.path
import re
import shared_pipe #settings shared between non-per-job scripts in the suite.

# what's this for?:
# Breaking up the progress file into N directories.
# Creating a seperate condor submit file for each batch of rangeSize directories

parentDir = '/z/Comp/kelesgroup/atsnp/JASPAR/TABLES'   
#how long does this end up taking?

#ultimate goal: 19TB of data:
#parentDir = '/z/Comp/kelesgroup/atsnp/ENCODE/BIGTABLES'
#parentDir = '/z/Comp/kelesgroup/atsnp/ENCODE/BIGTABLES/1268'

def get_file_list(path):
   fileCount = 0 
   fList = [] 
   for dirName, subdirList, fileList in os.walk(parentDir):
       for fname in sorted(fileList): 
            if re.match("db_\d+_\d+.*RData$", fname):
              fileCount += 1
              fpath = "/".join([dirName, fname])
              fList.append(fpath)
   return fList

#Copies source for all per-job scripts and segments of the overall list 
#of files to transfer.
def setupJobDirs(input_path):
    shared_pipe.init() 
    filesToCopy = ['multi_pipeline.py', 
                   'rdata2sqlite.R', 
                   'count_records_matching_cutoffs.py',
                   'shared_pipe.py']

    for i in range(0, shared_pipe.SETTINGS['chunk_count']):
        jobDir = 'chunk' + str(i).zfill(2)
        if not os.path.exists(jobDir):
            os.makedirs(jobDir)
        else:
            print "Directory : " + jobDir + " already exists."
            print "delete all chunk* directories to use this script."
            exit(1)
        for oneFile in filesToCopy:
            shutil.copyfile(oneFile, jobDir + "/" + oneFile)

    fList = get_file_list(input_path)    
    chunk_size = float(len(fList)) /    \
                float((shared_pipe.SETTINGS['chunk_count']))
    chunk_size = math.ceil(chunk_size)
    print "chunk count = " + str(shared_pipe.SETTINGS['chunk_count'])
    print "this many files in the list: " + str(len(fList))
    print "chunk size: " + str(chunk_size)

    progressFile = None 
    chunk = 0 

    #should list files 0 to N-1. Not 1 to N
    for index, oneFile in enumerate(fList):
       #print "index = " + str(index)
       if index % chunk_size == 0 or index == 0: 
           if progressFile is not None: 
               progressFile.close()
               chunk += 1
           progPath = 'chunk' + str(chunk).zfill(2) + '/progress.txt'
           print "setting up a new progress file: " + progPath
           progressFile = open(progPath, 'ar+')
       oneLine = " ".join([oneFile, 
                           str(shared_pipe.PROGRESS_STATES['NOT_STARTED']),
                           "\n"])
       progressFile.write(oneLine) 

    if progressFile is not None:
        progressFile.close()
        #close the last progress file that is written.

def setupCondorSubmitFiles():
    submitSize = (shared_pipe.SETTINGS['chunk_count'] )/ \
                 (shared_pipe.SETTINGS['n_submit_files'])
    print "setting up condor submit files. Each file to handle " + \
          str(submitSize) + " jobs"
    exe  = 'executable         = /s/bin/python'
    args = 'arguments          = "multi_pipeline.py"'
    idir = 'initialdir         = chunk$INT(ChunkIndex,%02d)'
    err =  'error              = errors.out'
    rank=  'Rank               = Memory >= 40000'
    q   =  'queue ' + str(submitSize) + "\n"
    for si in range(0,shared_pipe.SETTINGS['n_submit_files']):
        submitRangeStart = si * submitSize
        submitRangeEnd = ((si + 1) * submitSize ) - 1
        #sumbmit_size many jobs per condor submit file.
        comment = " ".join(["#Submit file", str(si), "for chunks", 
                  "from", str(submitRangeStart), "to", str(submitRangeEnd)])
        indx = 'ChunkIndex         =$(Process)+' + str(submitRangeStart) 
        submitContent = "\n".join([comment,exe,args,indx,idir,err,rank,q])
        submitFile = open('condor-submit' + str(si).zfill(2) + '.sub', 'ar+')
        submitFile.write(submitContent)
        submitFile.close()

   
print "Setting up directories and submit files for: " + parentDir 

setupJobDirs(parentDir)
setupCondorSubmitFiles()

print "job directories have been setup"

