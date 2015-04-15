#!/usr/bin/env python

from store import * # part 1
from btree import * # part 2

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

def initDB(name, schema=None):
    db = bptree(schema=schema, name=name)
    if os.path.exists(db.path):
        pass # open DB
    else:
        print "creating default file"
        f = open(db.path, "w+b")
        #print pickle.dumps(db)
        pickle.dump(db, f)
        f.close()

    if not os.path.exists(db.pathdir):
        os.makedirs(db.pathdir)

    return db

if __name__ == "__main__":
    print "This file holds definitions and should not be executed direfctly."
