import os
import shutil
import os.path
import socket
import shared_pipe
#what's this for?:
#This script is supposed to manage a progress file that counts
#how many recoreds fall within (under) certian combinations of pvalue 
#thresholds. 

#This script expects to be able to find a progress file in each job directory.

#Read one line of progress file and return the data.
#updates the summary dict (counts) based on what's in 'terms'
def updateCountsForOneLine(terms, summary):
    shared_pipe.init()
    #already been added. cutoffs = shared_pipe.NAMED_CUTOFFS.keys() 
    #handle the 'file based' or 'file level' counts.
   
    #what is status?    #I get the status as a number off of the line. 
    pgs = dict(zip(shared_pipe.PROGRESS_STATES.values(),
                   shared_pipe.PROGRESS_STATES.keys()  ) )
    status = pgs[int(terms[1])]   #status should be the name of the state.
    #print "determined this is the progress state for this line ; " + status 
    #print "summary now :" + str(summary)
    summary[status] += 1 
 
    #then handle the 'record-level' counts.
       #look for these counts; ordered by their index in the list of keys  
       #in the NAMED_CUTOFFS dict.
    if status == 'COMPLETE': 
        specific_counts = terms[2:]
        i = 0
        while i < len(specific_counts):
          cutoff_name = specific_counts[i]
          value = int(specific_counts[i+1].replace(':', '') )
          summary[cutoff_name] += value    #this is where we are/were failing to accumulate.
          i += 2 
    return summary 

def readOneProgressFile(progFilePath):
    summary = getEmptyCounts()
    fileLines = []
    try:
        progFile = open(progFilePath,  'r')
    except IOError:
        #This file is not here.    
        return {"summary" : summary, 
                'fileLines': fileLines } 
    for line in progFile:
        fileLines.append(line)
        terms = line.split()
        summary.update(updateCountsForOneLine(terms, summary))
        #counts should be read and added up here.
        #does the code that replaces this in updateCountsForOneLine work?
        #if it does, then REMOVE what is below.
    progFile.close()
    #There is one progress file per directory, so this should contain
    #the summary dict. Summary contains only counts of various things.
    #j (don't mess with the fileLines)
    return { "summary" : summary,  
             "fileLines" : fileLines }

def checkOneDirectory(jobDir):
    jobProgressFile = jobDir + "/" + 'progress.txt'
    jobSummary = readOneProgressFile(jobProgressFile)
    #print "summary for job at : " + jobDir
    #print str(jobSummary['summary'])
    return jobSummary

#return True if this directory has completed, False if it has not.
def checkForCompleteDir(oneSummary):
    if oneSummary['IN_PROGRESS'] == 0 and oneSummary['NOT_STARTED'] == 0:
        return True
    return False

#add the numbers from dirResults to the numbers from 
#overallSummary. Return overallSummary.
def update_overall_counts(overallSummary, dirResults):
    #dirResults don't contain the requisite keys yet...
    #print "dirResults : " + repr(dirResults['counted'])
    #print "overallSummary " + repr(overallSummary['counted'])
    #overallSummary is what gets written per directory.
    #counts = overallSummary
    for one_count in overallSummary.keys():
        overallSummary[one_count] += dirResults[one_count]
    return overallSummary  

def getEmptyCounts():
    shared_pipe.init()
    counts = shared_pipe.NAMED_CUTOFFS.keys() 
    counts.extend(['COMPLETE', 'IN_PROGRESS', 'NOT_STARTED' ])
    counts.extend(['rdata', 'counted'])
    #print "counts : "+ repr(counts)
    overallSummary =  dict.fromkeys(counts, 0)
    return overallSummary
 
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

    overallSummary = getEmptyCounts()
    
    for i in range(0, shared_pipe.SETTINGS['chunk_count']):

        dirName = 'chunk' + str(i).zfill(2)
        dirResults = checkOneDirectory(dirName)
  
        #Does the following line effectively replace the block of code that follows?
        #print "right before the call to update_overall_counts" + str(overallSummary)
        overallSummary = update_overall_counts(overallSummary, dirResults['summary'])
        print "summary count ; " + str(dirResults['summary']['counted'])

        #This is where all of the special stats get added.
        overallProgressFile.writelines(dirResults['fileLines'])

        if checkForCompleteDir(dirResults['summary']):
            completedDirs += 1 

    completedChunks = completedDirs / submitSize
    print 'completed counting ' + str(completedDirs) + " out of " + \
          str(shared_pipe.SETTINGS['chunk_count']) + " directories"
    print "completed counting " + str(completedChunks) + " out of " +\
          str(shared_pipe.SETTINGS['n_submit_files']) + " submit files."
    #pretty sure that we should use the submit file called 'completedChunks' next.
    overallProgressFile.write('summary for all active jobs:')
    overallProgressFile.write(str(overallSummary))
    overallProgressFile.close()
    
print "Running a status check..... " 
checkAllDirs()
print 
print "Completed the status check!"
