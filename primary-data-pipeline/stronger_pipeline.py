import os
import shutil
import fileinput
import os.path
import re
import time #for use w/ time.sleep(5)
import socket
import subprocess
import string
from sqlite2elasticsearch import run_single_file

#TODO: Goddamned consistent style.

#used to track progress in 
pStates = { 'NOT_STARTED' : 0, 
            'IN_PROGRESS' : 1, 
            'COMPLETE'    : 2 }

#parent directory to work with.
#TODO: ensure this works well for the actual parent dir, not this lil' subset.
#reloading is kind of underway 
parentDir = '/z/Comp/kelesgroup/atsnp/JASPAR/BIGTABLES'
#parentDir = '/z/Comp/kelesgroup/atsnp/JASPAR/BIGTABLES/2'
#parentDir = '/z/Comp/kelesgroup/atsnp/ENCODE/BIGTABLES/1268'
#parentDir = '/z/Comp/kelesgroup/atsnp/ENCODE/BIGTABLES'

def createLockfile(atPath):
   print "creating lockfile : " + atPath
   lockFile = open(atPath, 'ar+')
   lockFile.write(socket.gethostname())
   lockFile.close() 

def deleteLockfile(atPath):
   print "removing lockfile " + atPath
   if os.path.isfile(atPath):
       os.remove(atPath)

#if the lockfile at the specified path exists, 
#wait for it to go away.
def getLockOnFile(atPath):
    while True:
        if not os.path.isfile(atPath):
           break 
        else: 
           readLock = open(atPath, "r")
           workingHost = readLock.read()
           print "lockfile at " + atPath + \
                 " is still there. host=" + workingHost
           readLock.close()
           time.sleep(10)
    createLockfile(atPath)


#used only when there's not yet a progress file
def writeProgressFile(openFile):
    fileCount = 0
    #make a list in a file of the files to work with.
    #TODO: remove the clockouts at n = 200
    for dirName, subdirList, fileList in os.walk(parentDir):
        for fname in fileList: 
            if re.match(".*RData$", fname):
              fileCount += 1
              fpath = "/".join([dirName, fname])
              oneLine = " ".join([fpath, str(pStates['NOT_STARTED']), "\n"])
              openFile.write(oneLine)
    print "fileCount: " + str(fileCount)
     

#if this function fails, the R script's output has been changed  
def siftROutput(rResult):
    rowCount = None
    outfileName = None
    for line in rResult.split("\n"):
       if re.search(r'output_file', line):
           outfileName = line.split()[2]
           outfileName = outfileName.translate(None, '"')
       elif re.search(r'N=', line):
           rowCount = int(re.findall(r'(\d+)', line)[1])
    #print "N rows in R data file :  " + str(rowCount)
    #print "output filename : " + outfileName
    return { "row_count" : rowCount,
             "outfile_name" : outfileName }


#This function assumes that this host has a lock on the progress file.
#return the path to a file that will be worked on.
#update the progress file to indicate what work is being done.
def analyzeProgressFile(progPath):
   #progFile = open(progPath, 'r+') 
   filePicked = None 
   terms = None
   summary = { 'COMPLETE' : 0, 'IN_PROGRESS' : 0, 'NOT_STARTED' : 0 } 
   #all print statements in the loop will be written back into the original file
   #https://docs.python.org/2/library/fileinput.html?highlight=fileinput#fileinput
   tempFilePath  = progPath + '.tmp'
   tempFile = open(tempFilePath ,'w')
   progFile = open(progPath,  'r')
   for line in progFile:
       changedLine = False
       terms = line.split()
       #print(str(terms))
       if int(terms[1]) == pStates['COMPLETE']:
           summary['COMPLETE'] += 1
           #print "file : " + terms[0] + " has been run"
       elif int(terms[1]) == pStates['IN_PROGRESS']:
           summary['IN_PROGRESS'] += 1
           if terms[2].strip() == socket.gethostname().strip():
               print "found a file on host: " + socket.gethostname()
               if filePicked is None:
                   print "picking up in progress file " + terms[0]
                   filePicked = terms[0]
               else: 
                   print "file w/ matching host name, but already picked file"
           else: 
               print "skipping in progress file " + terms[0] + " host working is: " + terms[2]
       elif int(terms[1]) == pStates['NOT_STARTED'] :
           if filePicked is None:
                filePicked = terms[0]
                tempFile.write( " ".join([terms[0],
                               str(pStates['IN_PROGRESS']), 
                               socket.gethostname()+"\n"]))
                changedLine = True
                summary['IN_PROGRESS'] += 1
           else:
               summary['NOT_STARTED'] += 1

       if not changedLine: 
           tempFile.write(line)

   tempFile.close(); progFile.close()
   shutil.move(tempFilePath, progPath)
   return { "summary" : summary,  
            "fileToProcess" : filePicked }
       #DONE when fileToProcess is None.


def markOneFileAsComplete(progPath, pathToFile, allCounts):
   #mark the file as complete 
   print "marking file " + pathToFile + " as complete"
   tempFilePath  = progPath + socket.gethostname() + '.tmp'
   tempFile = open(tempFilePath ,'w')
   progFile = open(progPath,  'r')
   counts = "rdata: " + str(allCounts['rdata'])
   counts += " elastic added " + str(allCounts['es_added'])
   counts += " elastic rejected " + str(allCounts['es_rejected'])
   #add line count data to the COMPLETE line
   for line in progFile:
       terms = line.split(" ")
       if terms[0] == pathToFile:
          tempFile.write(" ".join([terms[0], 
                                  str(pStates['COMPLETE']),
                                  counts+"\n"]))
       else:
           tempFile.write(line)
   tempFile.close(); progFile.close()
   shutil.move(tempFilePath, progPath)


#processing includes the following:
# 1. select a file to work on. 
# 2. copy the selected file to /scratch/atsnp-pipeline
# 3. run the R script to convert /scratch/atsnp-pipeline/Rdatafile
#    into /scratch/atsnp-pipeline/sqliteFile
# 4. run another script that sucks up the contents of the SQLite file into ES.
# 5. mark that file as DONE and continue through the loop
def processOneFile(pathToFile):
    localWorkingDir = 'scratch/atsnp-pipeline'
    if not os.path.exists(localWorkingDir):
        os.makedirs(localWorkingDir)
    rDataFile = os.path.basename(pathToFile)
    workingPath = os.path.join(localWorkingDir, rDataFile)
    print "copying file : " + pathToFile + " to : " + workingPath

    shutil.copy(pathToFile, workingPath)
    print "copied field at workingPath"
    cmd = " ".join(['/s/bin/Rscript', 'rdata2sqlite.R', 
                    workingPath, localWorkingDir])

    result = subprocess.check_output(cmd, shell=True)
    r_output = siftROutput(result)
    rows_from_rdata = r_output["row_count"]
    sqliteFile =  r_output["outfile_name"]
    print "rows processed by RScript: " + str(rows_from_rdata)

    elastic_rows = run_single_file(sqliteFile)
    oneFileCounts = { 'rdata'    :  rows_from_rdata,
                      'es_added' :  elastic_rows['added'],
                      'es_rejected' : elastic_rows['rejected'] } 
                       #duplicates are rejected.
    return oneFileCounts  #summary contanis 'added' and 'rejected' keys
   
 
print "beginnig to run pipeline on " + socket.gethostname()

filesStillToProcess = True
#if the progress.txt file is gone, this will start from the top.
pathToProg = '../progress.txt'
progLock = pathToProg + ".lk"

#as-yet unused.
pathToSummary = 'summary.txt'
summaryLock = pathToSummary + ".lk"

#create the progress file if it does not exist.
if not os.path.isfile(pathToProg):
   progressFile = open(pathToProg, 'ar+') 
   writeProgressFile(progressFile)
   progressFile.close()
   exit(1)
while filesStillToProcess:

    getLockOnFile(progLock) #in this block; all the analysis must occur
    progressData = None

    progressData = analyzeProgressFile(pathToProg) 
    print "progress " + str(progressData)
    deleteLockfile(progLock)  #should be done once an action is selected.

    if progressData['fileToProcess'] is None:
        print "out of files summary : " + str(progressData["summary"])
        filesStillToProcess = False 
        print "cleaning up scratch working directory" 
        localWorkingDir = 'scratch/atsnp-pipeline'
        if os.path.exists(localWorkingDir):
            shutil.rmtree(localWorkingDir)
    else:
        #this will take a bit; don't hog the lock on the progress file during this time.
        print "file to process" + progressData['fileToProcess']

        results = processOneFile(progressData['fileToProcess'])
        #results should contain info from R and from indexing into ES.

        #mark off the progress.
        getLockOnFile(progLock)
        markOneFileAsComplete(pathToProg, 
                              progressData['fileToProcess'],
                              results)
        deleteLockfile(progLock)

