import os, sys, shutil
import os.path
import socket
import shared_pipe
from shared_pipe import WhichTFLibs, print_with_color

#what's this for?:
#Checking the overall progress of a batch of atsnp data being 
#ingested into the project's elasticsearch cluster.
#You should be able to run this script at any time and get counts
#of how many files have been successfully ingested, as well as
#information about the status of running/crashed jobs.

#This script expects to be able to find a progress file in each job directory.

#shared_pipe.init()  #can this just be called once at the top of the file?
#Code adapted from the counting pipeline.

def getEmptyCounts():
    shared_pipe.init()
    #The status 'es_skipped' should ALWAYS indicate that a record with that
    #ID was already present in the Elasticsearch index.
        #cutoff total is the number of records that meets the pvalue cutoff.
        #if pvalue cutoffs are disable, this number will be 0.
    counts = ['COMPLETE', 'IN_PROGRESS', 'NOT_STARTED',
              'rdata', 'cutoff_total', 'es_added', 'es_skipped', 'other']
    return dict.fromkeys(counts, 0)


#Have to successfully parse the new format of the comppleted progress
#file lines.
def addCountsFromOneLine(terms, summary):
    shared_pipe.init()   #can this be factored to 1 call at the top of the file? 
    pgs = dict(zip(shared_pipe.PROGRESS_STATES.values(), 
                   shared_pipe.PROGRESS_STATES.keys() ) ) 
    status = pgs[int(terms[1])]
    summary[status] += 1

    if status == 'COMPLETE':
        specific_counts = terms[2:]
        #print "specific counts: " + repr(specific_counts)
        i = 0
        while i < len(specific_counts):
           value_called = specific_counts[i]
           #print "value called " + value_called
           value = int(specific_counts[i+1].replace(':', ''))
           #print "value : "  + str(value)
           summary[value_called] += value
           i += 2 
    return summary 
  

def readOneProgressFile(progFilePath):
    shared_pipe.init()
    summary = getEmptyCounts()
    progFile = open(progFilePath, 'r')
    fileLines = []
    for line in progFile:
        fileLines.append(line)
        terms = line.split()
        summary.update(addCountsFromOneLine(terms, summary))
    progFile.close()
    return { "summary" : summary,  
             "fileLines" : fileLines }


def checkOneDirectory(jobDir):
    jobProgressFile = jobDir + "/" + 'progress.txt'
    jobSummary = readOneProgressFile(jobProgressFile)
    #serious DEBUG output:
    #print "summary for job at : " + jobDir
    #print str(jobSummary['summary'])
    return jobSummary

#return True if this directory has completed, False if it has not.
def checkForCompleteDir(oneSummary):
    if oneSummary['IN_PROGRESS'] == 0 and oneSummary['NOT_STARTED'] == 0:
        return True
    return False

def update_overall_counts(overallSummary, dirResults):
    for one_count in overallSummary.keys():
        overallSummary[one_count] += dirResults[one_count]
    return overallSummary


#There's a cleaner, more pythonic way to do this.
#(I'd rather have a list than a dict, as it's inherently ordered)
def setup_list_of_submit_files(how_many_submit_files):
    return [False for i in range(0, how_many_submit_files)]

def show_which_batches_are_done(batches):
    doneBatches = 0
    for i, one_batch in enumerate(batches):
        msg = "\tStatus for batch launched by submit file " + str(i) + " is:"
        if one_batch is True:
            msg += " complete"    
            doneBatches += 1
        else: 
            msg += " incomplete."
        print msg
    return doneBatches
  

def writeToFileAndPrint(msg, fileToWrite):
    if type(msg).__name__ == 'list':
        fileToWrite.writelines(msg)
        for x in msg:
            print x
    else:
        print msg
        fileToWrite.write(msg)
 
#This is assuming that we are running the submit files in order.
#(Dangerous assumption: working now on a simple fix.)
def checkAllDirs(parent):
    shared_pipe.init() 

    #number of job dirs per condor submit file.
    submitSize = (shared_pipe.SETTINGS['chunk_count'] )/ \
                 (shared_pipe.SETTINGS['n_submit_files'])
    completedDirs = 0

    overallProgressFile = open('progress.txt', 'ar+')
    #should be regenerated once per status check run, regardless of which 
    #data sets are being checked.

    overallSummary = getEmptyCounts()
    k = 0; completeDirsForBatch = 0 #k counts directories in a batch.
    whichBatch = 0 
    batches = setup_list_of_submit_files(shared_pipe.SETTINGS['n_submit_files'])
     
    for i in range(0, shared_pipe.SETTINGS['chunk_count']):
        dirName = '/'.join([parent, 'chunk' + str(i).zfill(2)])
        dirResults = checkOneDirectory(dirName)
        overallSummary = update_overall_counts(overallSummary, 
                                               dirResults['summary'])
        #writeToFileAndPrint(dirResults['fileLines'], overallProgressFile)

        #overallProgressFile.writelines(dirResults['fileLines'])
        k += 1 #This will get reset once per batch.
        if checkForCompleteDir(dirResults['summary']):
            completedDirs += 1 
            completeDirsForBatch += 1 

        #End of batch turn over all per-batch calculations.
        if k == submitSize: 
            if completeDirsForBatch == submitSize: 
                batches[whichBatch] = True        
            k = 0; completeDirsForBatch = 0; whichBatch += 1 

    how_many_complete_batches = show_which_batches_are_done(batches)
    writeToFileAndPrint( "\tcompleted " + str(completedDirs) + " out of " + \
          str(shared_pipe.SETTINGS['chunk_count']) + " directories.",        \
         overallProgressFile)
    writeToFileAndPrint("\tcompleted " + str(how_many_complete_batches) + " out of " +\
          str(shared_pipe.SETTINGS['n_submit_files']) + " submit files.",             \
          overallProgressFile)

    #pretty sure that we should use the submit file called 'completedChunks' next.
    writeToFileAndPrint('  summary for all active jobs:' ,  overallProgressFile)
    writeToFileAndPrint(str(overallSummary) , overallProgressFile)
    overallProgressFile.close()

def clear_any_existing_progress_file():
    if os.path.isfile('progress.txt'): 
        print "Replacing overall progress file..."
        os.remove('progress.txt')


libs_to_run = WhichTFLibs(sys.argv).run_these
print "Collecting status information for the following data sets: " 
print ' and '.join(libs_to_run)

clear_any_existing_progress_file()
overallSummary = getEmptyCounts()

for one_data_set in libs_to_run:
    print ' '.join(["\n\tRunning status check for",one_data_set,"...."])
    if os.path.exists(one_data_set): 
        checkAllDirs(one_data_set)
    else:
        msg= "Skipped status check for " + one_data_set + ". Its not there."
        print_with_color(msg, why='warn')

msg = "Completed status check"
print_with_color(msg, why='success')

