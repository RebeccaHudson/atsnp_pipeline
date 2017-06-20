import os
import shutil
import os.path
import re
import socket
import subprocess
import string
from count_records_matching_cutoffs import run_single_file
import shared_pipe

#what's this for?:
#Instead of each condor job trying to update the same progress file, 
#each job should refer to a progress file in its own directory.
#use setup_progress_files.py parent_dir N to prepare progress files.

#used to track progress in progress files.
pStates = { 'NOT_STARTED' : 0, 
            'IN_PROGRESS' : 1, 
            'COMPLETE'    : 2 }

#ultimate goal.
#parentDir = '/z/Comp/kelesgroup/atsnp/ENCODE/BIGTABLES'


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


#update the progress file to indicate what work is being done.
def analyzeProgressFile(progPath):
   filePicked = None 
   terms = None
   summary = { 'COMPLETE' : 0, 'IN_PROGRESS' : 0, 'NOT_STARTED' : 0 } 
   tempFilePath  = progPath + '.tmp'
   tempFile = open(tempFilePath ,'w')
   progFile = open(progPath,  'r')
   for line in progFile:
       changedLine = False
       terms = line.split()
       #print(str(terms))
       if int(terms[1]) == pStates['COMPLETE']:
           summary['COMPLETE'] += 1
       elif int(terms[1]) == pStates['IN_PROGRESS']:
           #if there is an in-progress file, pick that one.
           filePicked = terms[0]
           summary['IN_PROGRESS'] += 1
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

def allCountsForOneFileAsString(allCounts):
    shared_pipe.init()
    build_line = ""
    counts = ['rdata', 'counted']
    counts.extend(shared_pipe.NAMED_CUTOFFS.keys())
    for one_count in counts:
        build_line += " " + one_count + " :" +  str(allCounts[one_count])
    return build_line


def markOneFileAsComplete(progPath, pathToFile, allCounts):
   #mark the file as complete 
   writeToJobLog("marking file " + pathToFile + " as complete")
   tempFilePath  = progPath + socket.gethostname() + '.tmp'
   tempFile = open(tempFilePath ,'w')
   progFile = open(progPath,  'r')
   counts_as_str = allCountsForOneFileAsString(allCounts)
   #add line count data to the COMPLETE line
   for line in progFile:
       terms = line.split(" ")
       if terms[0] == pathToFile:
          tempFile.write(" ".join([terms[0], 
                                  str(pStates['COMPLETE']),
                                  counts_as_str+"\n"]))
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
    localWorkingDir = 'scratch'
    if not os.path.exists(localWorkingDir):
        os.makedirs(localWorkingDir)
    rDataFile = os.path.basename(pathToFile)
    workingPath = os.path.join(localWorkingDir, rDataFile)
    writeToJobLog( "copying file : " + pathToFile + " to : " + \
                       workingPath + "\n")

    shutil.copy(pathToFile, workingPath)
    #print "copied field at workingPath"
    cmd = " ".join(['/s/bin/Rscript', 'rdata2sqlite.R', 
                    workingPath, localWorkingDir])

    result = subprocess.check_output(cmd, shell=True)
    r_output = siftROutput(result)
    rows_from_rdata = r_output["row_count"]
    sqliteFile =  r_output["outfile_name"]
    writeToJobLog("rows processed by RScript: " + str(rows_from_rdata)+"\n")
    oneFileCounts = run_single_file(sqliteFile)
    oneFileCounts.update({'rdata' : rows_from_rdata })
    #TODO: change 'run_single_file' so it specifically reads and handles the 
    writeToJobLog("cleaning up file at " + workingPath +"\n")
    writeToJobLog("cleaning up sqlite file at " + sqliteFile +"\n")
    os.remove(sqliteFile)
    os.remove(workingPath)
    return oneFileCounts  #summary contanis 'added' and 'rejected' keys
   

def writeToJobLog(message):
    jobLogFile = open('joblog.txt', 'a+') 
    jobLogFile.write(message)
    jobLogFile.close()

writeToJobLog("beginnig to run pipeline on " + socket.gethostname() + "\n")
filesStillToProcess = True
pathToProg = 'progress.txt'

while filesStillToProcess:
    progressData = None
    progressData = analyzeProgressFile(pathToProg) 
    writeToJobLog("progress " + str(progressData) + "\n")

    if progressData['fileToProcess'] is None:
        writeToJobLog( "out of files summary : " + \
                         str(progressData["summary"]) +"\n")
        filesStillToProcess = False 
        writeToJobLog("cleaning up scratch working directory"+"\n")
        localWorkingDir = 'scratch'
        if os.path.exists(localWorkingDir):
            shutil.rmtree(localWorkingDir)
    else:
        writeToJobLog("file to process" + progressData['fileToProcess'] + "\n")
        results = processOneFile(progressData['fileToProcess'])
        markOneFileAsComplete(pathToProg, 
                              progressData['fileToProcess'],
                              results)

