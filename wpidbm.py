#!/usr/bin/env python

import os       # file operations
import pickle   # serializing data structures
import fcntl    # exclusion of simultaneous edits.
import sys
import string

#from store import * # part 1
#from btree import * # part 2
from dbmglobals import *

# These each hold the methods used by each part
import part1 # interactive runs and test, 
import part3 # reading from csv and running queries
import part4 # reading from csv, creating logged transactions, and replaying

#==============================================================================#


#==============================================================================#
    
# Running the value store
if __name__ == "__main__":
    if len(sys.argv) > 1:
        part1.interactive()
    else:
        part3.csvs()
        part4.part4()
        
        
