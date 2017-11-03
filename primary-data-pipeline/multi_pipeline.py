import os
import shutil
import os.path
import re
import socket
import subprocess
import string
from process_and_index import process_data_from_one_file 
import shared_pipe
import rpy2.robjects as robjects

shared_pipe.init()
pStates = shared_pipe.PROGRESS_STATES
cutoffs = shared_pipe.PVALUE_CUTOFFS
use_cutoffs = shared_pipe.RESTRICT_BY_PVALUE

#which_lib = os.path.abspath('..').split('/')[-1] #must be either 'encode' or 'jaspar'
#parentDir = shared_pipe.PARENT_DIRS[which_lib]
#used only when there's not yet a progress file
#THIS script does not setup a progress file.
#def writeProgressFile(openFile):
#    fileCount = 0
#    #make a list in a file of the files to work with.
#    #TODO: remove the clockouts at n = 200
#    for dirName, subdirList, fileList in os.walk(parentDir):
#        for fname in fileList: 
#            if re.match(".*RData$", fname):
#              fileCount += 1
#              fpath = "/".join([dirName, fname])
#              oneLine = " ".join([fpath, str(pStates['NOT_STARTED']), "\n"])
#              openFile.write(oneLine)
#    #print "fileCount: " + str(fileCount)

#Moved here from sqlite2elasticsearch.py
def setup_cutoffs_clause(name_of_data_frame):
    clause_parts = []
    for pvalue in cutoffs.keys():
        clause_part = ''.join(['(', name_of_data_frame, '$', pvalue, \
                         '<=', str(cutoffs[pvalue]), ')' ])
        clause_parts.append(clause_part)
    clause = ''.join(['[', '|'.join(clause_parts), ',]'])
    #print "does this make sense rt now? " + clause
    return clause 

#Moved here from sqlite2elasticsearch.py
#use cutoffs will matter for this.
def setup_query_to_pull_records(name_of_data_frame):
    query = name_of_data_frame
    if use_cutoffs:  #setup at the top of the file.
        query += setup_cutoffs_clause(name_of_data_frame)
        #append the where clause.
    print "pulling records w: " + query
    return query

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

def buildLineForCompletedFile(allCounts):
   countsLine = ""#"rdata: " + str(allCounts['rdata'])
   #cutoff_total is the nuumber of records that matched the cutoff; 
   #   this total is returned from a query of the sqlite3 temp file.
   countsToCheck = ['rdata', 'cutoff_total', 'es_added', 'es_skipped', 'other']
   for onecount in countsToCheck:
       countsLine += ' '.join([onecount, str(allCounts[onecount])])
       countsLine += " " 
   #counts += "es_added" + str(allCounts['es_added'])
   #counts += "es_skipped " + str(allCounts['es_rejected'])
   #counts += "other? : " + str(allCounts['other']) #should be EMPTY!
   #print "going to represent counts like this: " + countsLine
   return countsLine


def markOneFileAsComplete(progPath, pathToFile, allCounts, jobLogFile):
   #mark the file as complete 
   jobLogFile.write("marking file " + pathToFile + " as complete")
   tempFilePath  = progPath + socket.gethostname() + '.tmp'
   tempFile = open(tempFilePath ,'w')
   progFile = open(progPath,  'r')
   #the text to record the numeric counts from processing each file.
   #print "in markOneFileAsComplete."
   #print "allCounts before calling buildLineForCompletedFile: " + repr(allCounts)
   counts = buildLineForCompletedFile(allCounts)

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
#
#    replace this step
# 3. run the R script to convert /scratch/atsnp-pipeline/Rdatafile
#    into /scratch/atsnp-pipeline/sqliteFile
#
# 4. run another script that sucks up the contents of the SQLite file into ES.
# 5. mark that file as DONE and continue through the loop
def processOneFile(pathToFile, jobLogFile):
    localWorkingDir = 'scratch'
    if not os.path.exists(localWorkingDir):
        os.makedirs(localWorkingDir)
    rDataFile = os.path.basename(pathToFile)

    workingPath = os.path.join(localWorkingDir, rDataFile)
    jobLogFile.write( "copying file : " + pathToFile + " to : " + \
                       workingPath + "\n")
    shutil.copy(pathToFile, workingPath)

    #This is where to open the file in rpy2!
    #I think this is for full ones: name_of_data = 'atsnp.bigtables'
    #from testing: name_of_data_frame = 'atsnp.scraptables'
    name_of_data_frame = 'atsnp.bigtables'

    #rd = robjects.r['load']('scrap-bigtables.RData')
    print "path to file: " + pathToFile
    rd = robjects.r['load'](pathToFile)

    #all_rows = robjects.r(name_of_data)
    rows_from_rdata = robjects.r('nrow(' + name_of_data_frame + ')')[0]
    query_to_pull_records = 'records_to_use = ' \
                               + setup_query_to_pull_records(name_of_data_frame)
    robjects.r(query_to_pull_records)  #Assign 'records_to_use' from within R.
    rows_matching_cutoff = robjects.r('nrow(records_to_use)')[0]
    all_data_within_cutoff = robjects.r('records_to_use') #what's getting handed off.
    headers = robjects.r('names(' + name_of_data_frame + ')')

    jobLogFile.write("rows pulled by rpy2 from RData: " + str(rows_from_rdata)+"\n")

    #This should hand off the data itself.
    elastic_rows = \
      process_data_from_one_file(all_data_within_cutoff, headers)

    print "elastic rows " + str(elastic_rows)
    #'Matches cutoff' is a number taken from querying the sqlite3 temp file.
    #It's supposed to verify that the correct number of rows got indexed.
    oneFileCounts = { 'rdata'       : rows_from_rdata,
                      'cutoff_total': rows_matching_cutoff,
                      'es_added'    : elastic_rows['added'],
                      'es_skipped'  : elastic_rows['skipped'],
                      'other'       : elastic_rows['other'] }
                       #'cutoff_total': elastic_rows['matches_pval_cutoff'],
                       #'es_rejected' : elastic_rows['rejected'],  
                       #duplicates are rejected!
    jobLogFile.write("cleaning up file at " + workingPath +"\n")
    os.remove(workingPath)
    return oneFileCounts  #summary contanis 'added' and 'rejected' keys
 
jobLogFile = open('joblog.txt', 'ar+')
jobLogFile.write("beginnig to run pipeline on " + socket.gethostname() + "\n")
filesStillToProcess = True
pathToProg = 'progress.txt'

while filesStillToProcess:
    progressData = None
    progressData = analyzeProgressFile(pathToProg) 
    jobLogFile.write("progress " + str(progressData) + "\n")

    if progressData['fileToProcess'] is None:
        jobLogFile.write( "out of files summary : " + \
                         str(progressData["summary"]) +"\n")
        filesStillToProcess = False 
        jobLogFile.write("cleaning up scratch working directory"+"\n")
        localWorkingDir = 'scratch'
        if os.path.exists(localWorkingDir):
            shutil.rmtree(localWorkingDir)
    else:
        jobLogFile.write("file to process" + progressData['fileToProcess'] + "\n")
        #This is where this will run:
        results = processOneFile(progressData['fileToProcess'], jobLogFile)
        markOneFileAsComplete(pathToProg, 
                              progressData['fileToProcess'],
                              results,
                              jobLogFile)

