#!/bin/bash

#Run this script in the active terminal *before* you run 
#setup jobs and dirs. 

#This script allows you to run the pipeline scripts
#and have them create directories with the correct permissions.
#Without these commands, the files created by the setup script 
#will not be able to be changed or deleted by others in the group.

echo "Setting up group and umask for atsnp group."
umask 002
newgrp atsnp



