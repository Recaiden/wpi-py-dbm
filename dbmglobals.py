#!/usr/bin/env python

from store import * # part 1
from btree import * # part 2

import pickle

COMP_LT = "LESS_THAN"
COMP_GT = "GREATER_THAN"
COMP_EQ = "EQUAL_TO"
COMP_LE = "LESS_THAN_OR_EQUAL_TO"
COMP_GE = "GREATER_THAN_OR_EQUAL_TO"
COMP_ALL = "NO_CONDITIONS"

OPTIONS = ('p', 'g', 'r', 'pf', 'gf', 'q')
EXP_OPTIONS = ('Put: p key value',
               'Get: g key',
               'Remove: r key',
               'Put from file: pf key file',
               'Get to file: gf key file',
               'Quit')

#==========================================================================#
# Transactional modifications
#==========================================================================#
# Each of these operations separately locks the data-store and is a whole log-cycle
def Put(db, key, data):
    '''stores data under the given key.'''
    # acquire lock and open files
    marker = open("%s/%s"%(db.pathdir, key), 'w+b')
    handle = open(db.path, 'rb')
    
    db = pickle.load(handle)
    handle.close()
    handle = open(db.path, 'w+b')
    
    val = db.insert(key, data)
    pickle.dump(db, handle)
    
    # release lock and clean up
    marker.close()
    handle.close()
    
    return val

def Get(db, key):
    '''retrieves the data.'''
    # acquire lock and open files
    #marker = open(".%s"%key, 'w+b')
    marker = open("%s/%s"%(db.pathdir, key), 'w+b')
    handle = open(db.path, 'rb')
    db = pickle.load(handle)
    
    val =  db.get(key)
    
    handle.close()
    marker.close()
    
    return val

def Remove(db, key):
    '''deletes the key.'''
    # acquire lock and open files
    #marker = open(".%s"%key, 'w+b')
    marker = open("%s/%s"%(db.pathdir, key), 'w+b')
    handle = open(db.path, 'rb')
    
    # Update store from file, set new data, rewrite store to file
    db = pickle.load(handle)
    handle.close()
    handle = open(db.path, 'w+b')
    
    present = db.remove(key)
    pickle.dump(db, handle)
    
    # release lock and clean up
    marker.close()
    handle.close()
    
    return present

#==========================================================================#
# Intratransactional modifications
#==========================================================================#
# These methods should be used together, along with the
# inesrt/delete/update of the db to create a single transactions
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

#==========================================================================#
# Initialization
#==========================================================================#
def initDB(name, schema=None):
    db = bptree(schema=schema, name=name)
    if os.path.exists(db.path):
         handle = open(db.path, 'rb')
         db = pickle.load(handle)
         handle.close()
    else:
        print "creating default file"
        f = open(db.path, "w+b")
        #print pickle.dumps(db)
        pickle.dump(db, f)
        f.close()

    if not os.path.exists(db.pathdir):
        os.makedirs(db.pathdir)

    if not os.path.exists(db.pathlog):
        open(db.pathlog, 'a').close()

    return db

if __name__ == "__main__":
    print "This file holds definitions and utility methods and should not be executed directly."
