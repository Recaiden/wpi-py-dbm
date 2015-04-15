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

# Execution cycle
def openRelation(db):
    # acquire lock and open files
    handle = open(db.path, 'rb')

    db = pickle.load(handle)
    handle.close()
    handle = open(db.path, 'w+b')
    return handle, db

def getNext(db, key):
    val = db.get(key)
    return val

def closeRelation(db, handle):
    pickle.dump(db, handle)
    handle.close()

def tplFrom(db, columns, comparators, values):
    '''
    Return all tuples in the relation that match a given criterion

    Each of the below are parallel lists
    compataror is less than, greater than, or equal
    value is the value being comnpared to
    column is the key into each record.
    '''

    handle, db = openRelation(db)

    passed = []
    idx = 0
    keys = db.keys()
    valid = True
    
    while valid:
        tpl = getNext(db, keys[idx])
        idx += 1
        if idx >= len(keys):
            valid = False
        if not tpl:
            valid = False
            continue
        include = True
        for column, comparator, value in zip(columns, comparators, values):
            if comparator == COMP_LT:
                if int(tpl[column]) >= value:
                    include = False
            elif comparator == COMP_LE:
                if int(tpl[column]) > value:
                    include = False
            elif comparator == COMP_GE:
                if int(tpl[column]) < value:
                    include = False
            elif comparator == COMP_GT:
                if int(tpl[column]) <= value:
                    include = False
            if comparator == COMP_EQ:
                if tpl[column] != value:
                    include = False
        if include:
            passed.append(tpl)
        
    closeRelation(db, handle)
    return passed

def select(db, member, column, comparator, value):
    '''Return all the entries in a given colum that match a given criterion'''
    tpls = tplFrom(db, column, comparator, value)
    vals = []
    #print tpls
    for tpl in tpls:
        vals.append(tpl[member])
    return vals

#==============================================================================#
    
# Running the value store
if __name__ == "__main__":
    if len(sys.argv) > 1:
        part1.interactive()
    else:
        part3.csvs()
