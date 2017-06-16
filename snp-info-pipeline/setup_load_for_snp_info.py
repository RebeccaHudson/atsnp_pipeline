import os, grp, sys
import shutil
import os.path
import re
import shared_pipe #settings shared between non-per-job scripts in the suite.

shared_pipe.init()
parentDir = shared_pipe.SNP_INFO_PARENT_DIR  #'/z/Comp/kelesgroup/atsnp/SNP_INFO'

def get_effective_group():
    eguid = os.getegid()
    return grp.getgrgid(eguid).gr_name

if get_effective_group() != 'atsnp':
    msg = "\n****************************** STOP ***************************\n"
    msg += "Configure permissions by running this command: './setup_shell.sh'"+\
          "\n(Your user is supposed to have the primary group 'atsnp' active.)"
    print(msg)
    sys.exit(1)


def get_file_list(path):
   fileCount = 0 
   fList = [] 
   for dirName, subdirList, fileList in os.walk(parentDir):
       for fname in sorted(fileList): 
            #Yes, 'testDataN' IS the correct expression for these files.
            if re.match("testData\d+\.Rda$", fname):
              fileCount += 1
              fpath = "/".join([dirName, fname])
              fList.append(fpath)
   return fList

#Copies source for all per-job scripts and segments of the overall list 
#of files to transfer.
def setupJobDirs(input_path):
    shared_pipe.init() 
    filesToCopy = ['snp_info_pipeline.py', 
                   'process_snpinfo.R', 
                   'put_snp_info_in_es.py', 
                   'shared_pipe.py']

    for i in range(0, shared_pipe.SETTINGS['chunk_count']):
        print "setting up directory for " + str(i)
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
    print "fList :" + repr(fList)
    chunk_size = len(fList) /    \
                (shared_pipe.SETTINGS['chunk_count'] - 1)
    progressFile = None 
    chunk = 0 

    for index, oneFile in enumerate(fList):
       if index % chunk_size == 0 or index == 0: 
           if progressFile is not None: 
               progressFile.close()
               chunk += 1
           progPath = '/'.join(['chunk' + str(chunk).zfill(2), 'progress.txt'])
           progressFile = open(progPath, 'ar+')
       oneLine = " ".join([oneFile, 
                           str(shared_pipe.PROGRESS_STATES['NOT_STARTED']),
                           "\n"])
       progressFile.write(oneLine) 

    progressFile.close()  #close the last progress file that is written.
    print "this many files in the list: " + str(len(fList))
    print "chunk size: " + str(chunk_size)


def setupCondorSubmitFiles():
    submitSize = (shared_pipe.SETTINGS['chunk_count'] )/ \
                 (shared_pipe.SETTINGS['n_submit_files'])
    print "setting up condor submit files. Each file to handle " + \
          str(submitSize) + " jobs"
    exe  = 'executable         = /s/bin/python'
    args = 'arguments          = snp_info_pipeline.py'
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
print "about to setup submit files."
setupCondorSubmitFiles()

print "job directories have been setup"

