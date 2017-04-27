import os
import shutil
import os.path
import socket
import shared_pipe
#what's this for?:
#Checking the overall progress of a batch of atsnp data being 
#ingested into the project's elasticsearch cluster.
#You should be able to run this script at any time and get counts
#of how many files have been successfully ingested, as well as
#information about the status of running/crashed jobs.

#This script expects to be able to find a progress file in each job directory.

#used to track progress in progress files.
#shared_pipe.PROGRESS_STATES
#pStates = { 'NOT_STARTED' : 0, 
#            'IN_PROGRESS' : 1, 
#            'COMPLETE'    : 2 }

def readOneProgressFile(progFilePath):
    shared_pipe.init()
    summary = { 'COMPLETE' : 0, 'IN_PROGRESS' : 0, 'NOT_STARTED' : 0 } 
    progFile = open(progFilePath,  'r')
    fileLines = []
    for line in progFile:
        fileLines.append(line)
        terms = line.split()
        if int(terms[1]) == shared_pipe.PROGRESS_STATES['COMPLETE']:
            summary['COMPLETE'] += 1
        elif int(terms[1]) == shared_pipe.PROGRESS_STATES['IN_PROGRESS']:
            summary['IN_PROGRESS'] += 1
        elif int(terms[1]) == shared_pipe.PROGRESS_STATES['NOT_STARTED']:
           summary['NOT_STARTED'] += 1
    progFile.close()
    return { "summary" : summary,  
             "fileLines" : fileLines }

def checkOneDirectory(jobDir):
    jobProgressFile = jobDir + "/" + 'progress.txt'
    jobSummary = readOneProgressFile(jobProgressFile)
    print "summary for job at : " + jobDir
    print str(jobSummary['summary'])
    return jobSummary

#return True if this directory has completed, False if it has not.
def checkForCompleteDir(oneSummary):
    if oneSummary['IN_PROGRESS'] == 0 and oneSummary['NOT_STARTED'] == 0:
        return True
    return False

#This is assuming that we are running the submit files in order.
def checkAllDirs():
    shared_pipe.init() 

    #number of job dirs per condor submit file.
    submitSize = (shared_pipe.SETTINGS['chunk_count'] )/ \
                 (shared_pipe.SETTINGS['n_submit_files'])
    completedDirs = 0

    if os.path.isfile('progress.txt'): 
        print "Removing exinsting overall progress file to rebuild it.."
        os.remove('progress.txt')
    overallProgressFile = open('progress.txt', 'ar+')
    overallSummary = { 'COMPLETE' : 0, 'IN_PROGRESS' : 0, 'NOT_STARTED' : 0 } 

    for i in range(0, shared_pipe.SETTINGS['chunk_count']):

        dirName = 'chunk' + str(i).zfill(2)
        dirResults = checkOneDirectory(dirName)

        overallSummary['COMPLETE'] += dirResults['summary']['COMPLETE'] 
        overallSummary['IN_PROGRESS'] += dirResults['summary']['IN_PROGRESS'] 
        overallSummary['NOT_STARTED'] += dirResults['summary']['NOT_STARTED']     
        overallProgressFile.writelines(dirResults['fileLines'])

        if checkForCompleteDir(dirResults['summary']):
            completedDirs += 1 

    completedChunks = completedDirs / submitSize
    print 'completed ' + str(completedDirs) + " out of " + \
          str(shared_pipe.SETTINGS['chunk_count']) + " directories"
    print "completed " + str(completedChunks) + " out of " +\
          str(shared_pipe.SETTINGS['n_submit_files']) + " submit files."
    #pretty sure that we should use the submit file called 'completedChunks' next.
    overallProgressFile.write('summary for all active jobs:')
    overallProgressFile.write(str(overallSummary))
    overallProgressFile.close()
    
print "Running a status check..... " 
checkAllDirs()
print "Completed the status check!"
