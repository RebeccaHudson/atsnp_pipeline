import os
import shutil
import os.path
import re
import shared_pipe
import sys

# What does this file do? 
# Breaks up the whole colletion of files into N directories.
# Creating a seperate condor submit file for each batch of rangeSize directories
shared_pipe.init()
parentDir = shared_pipe.PARENT_DIR

def get_file_list(path):
   fileCount = 0 
   fList = [] 
   for dirName, subdirList, fileList in os.walk(parentDir):
       for fname in sorted(fileList): 
            if re.match("bigdb_\d+_\d+.*RData$", fname):
            #if re.match("db_\d+_\d+.*RData$", fname):
              fileCount += 1
              fpath = "/".join([dirName, fname])
              fList.append(fpath)
   return fList

def create_dir_or_fail(dirname, msg):
    if not os.path.exists(dirname):
        os.makedirs(dirname)
        return True
    else:
        print msg
        return False

#Copies source for all per-job scripts and segments of the overall list 
#of files to transfer.
def setupJobDirs(jaspar_or_encode, input_path):
    msg  = ' '.join(['Directory',jaspar_or_encode,'already exists.',
                    'Delete this directory to use this script.'])
    #Don't proceed in creating directories if the parent' isn't in.
    if not create_dir_or_fail(jaspar_or_encode, msg):
        print "Not setting up anything for " + jaspar_or_encode + \
              " because it already exists."
        return False

    #Message to fail with if some interesting data may be left over.
    #You have to go and manually delete this data. This script won't.
    msg = " ".join(["Directory already exists.",
              "delete all chunk<N> directories to use this script."])

    filesToCopy = ['multi_pipeline.py', 
                   'rdata2sqlite.R', 'shared_pipe.py',
                    'sqlite2elasticsearch.py']
    for i in range(0, shared_pipe.SETTINGS['chunk_count']): 
        jobDir = '/'.join([jaspar_or_encode,'chunk' + str(i).zfill(2)])
        if not create_dir_or_fail(jobDir, msg):
            return False #fail early, make the operator delete these.
        for oneFile in filesToCopy:
            shutil.copyfile(oneFile, jobDir + "/" + oneFile)

    fList = get_file_list(input_path)    
    chunk_size = len(fList) /    \
                (shared_pipe.SETTINGS['chunk_count'] - 1)
    progressFile = None 
    chunk = 0 
    print "how many files? " + str(len(fList))
    for index, oneFile in enumerate(fList):
       if index % chunk_size == 0 or index == 0: 
           if progressFile is not None: 
               progressFile.close()
               chunk += 1
           progPath = '/'.join([jaspar_or_encode, 
                               'chunk' + str(chunk).zfill(2), 
                               'progress.txt'])
           print "setting up a new progress file: " + progPath
           progressFile = open(progPath, 'ar+')
       oneLine = " ".join([oneFile, 
                           str(shared_pipe.PROGRESS_STATES['NOT_STARTED']),
                           "\n"])
       progressFile.write(oneLine) 

    progressFile.close()  #close the last progress file that is written.
    print "this many files in the list: " + str(len(fList))
    print "chunk size: " + str(chunk_size)
    return True #Indicate success.




#The tf_library argument is either 'JASPAR' or 'ENCODE'
def setupCondorSubmitFiles(tf_library):
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
        submitFileName = 'condor-submit' + str(si).zfill(2) + '.sub'
        path = '/'.join([tf_library, submitFileName])
        submitFile = open(path, 'ar+')
        submitFile.write(submitContent)
        submitFile.close()


#Uses the command line arguments to determine which datasets are run.
def which_datasets():
    run_these = []
    print "sys.argv  " + repr(sys.argv)
    print "len(sys.argv)" +  str(len(sys.argv))
    if len(sys.argv) >= 2 and len(sys.argv) <= 3:
       sets_to_run = sys.argv[1:]
       while len(sets_to_run) > 0:
           g = sets_to_run.pop()
           print "g: " + g
           if str.upper(g) not in ['ENCODE', 'JASPAR']:
               msg = "The only valid arguments are 'encode' and/or 'jaspar'\
                   (case insensitive). \n Enter 'jaspar' or 'encode' or both."
               print msg
               exit(1)
           else:
               run_these.append(str.lower(g))
    if len(run_these) == 0:
        print "Missing transcription factor library agrument(s).\n" + \
           "Specify encode or jaspar libraries to setup data loading for."
        exit(1)
    return run_these

#Is setting up directores for JASPAR or ENCODE. or both. 
#You will have to manually delete the directories to overwrite them.
libs_to_run = which_datasets()
ready = []; skipped = []
print "running these guys : " + repr(libs_to_run)  
for one_of_them in libs_to_run:
    parentDir = shared_pipe.PARENT_DIRS[one_of_them]
    print "Setting up directories and submit files for: " + parentDir 
    if not setupJobDirs(one_of_them, parentDir):
        print "Stuff is hanging around for " + one_of_them + \
              " not setting it up."
        skipped.append(one_of_them)
        continue
    ready.append(one_of_them)         
    setupCondorSubmitFiles(one_of_them)



if len(ready) > 0:
    to_run = ' and '.join(ready)
    print "Job directories have been setup. Ready to go for " + to_run

if len(skipped) > 0:
    passed_over = ' and '.join(skipped)
    print "Passed over requested setup for " + passed_over +\
          " due to processing files still sitting around."






