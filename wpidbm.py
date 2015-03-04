#!/usr/bin/env python

import os       # file operations
import pickle   # serializing data structures
import fcntl    # exclusion of simultaneous edits.
import sys
import random
import string

from store import * # part 1
from btree import * # part 2

db = None
filenDB="./wpi.db"
directory="wpi.db.locks"

OPTIONS = ('p', 'g', 'r', 'pf', 'gf', 'q')
EXP_OPTIONS = ('Put: p key value',
               'Get: g key',
               'Remove: r key',
               'Put from file: pf key file',
               'Get to file: gf key file',
               'Quit')

# These 3 operations hold the file-access/refreshing and attach to the actual data-store.
def Put(key, data):
    '''stores data under the given key.'''
    # acquire lock and open files
    marker = open("%s/%s"%(directory, key), 'w+b')
    handle = open(filenDB, 'rb')
    
    global db
    db = pickle.load(handle)
    handle.close()
    handle = open(filenDB, 'w+b')
    
    val = db.insert(key, data)
    pickle.dump(db, handle)

    # release lock and clean up
    marker.close()
    handle.close()
    
    return val

def Get(key):
    '''retrieves the data.'''
    # acquire lock and open files
    #marker = open(".%s"%key, 'w+b')
    marker = open("%s/%s"%(directory, key), 'w+b')
    handle = open(filenDB, 'rb')
    db = pickle.load(handle)
    
    val =  db.get(key)

    handle.close()
    marker.close()

    return val

def Remove(key):
    '''deletes the key.'''
    # acquire lock and open files
    #marker = open(".%s"%key, 'w+b')
    marker = open("%s/%s"%(directory, key), 'w+b')
    handle = open(filenDB, 'rb')

    # Update store from file, set new data, rewrite store to file
    global db
    db = pickle.load(handle)
    handle.close()
    handle = open(filenDB, 'w+b')
    
    present = db.remove(key)
    pickle.dump(db, handle)
    
    # release lock and clean up
    marker.close()
    handle.close()

def initDB():
    db = bptree()
    if os.path.exists(filenDB):
        pass # open DB
    else:
        print "creating default file"
        f = open(filenDB, "w+b")
        #print pickle.dumps(db)
        pickle.dump(db, f)
        f.close()

    if not os.path.exists(directory):
        os.makedirs(directory)

def randomKeyString():
    return ''.join(random.choice(string.ascii_uppercase) for i in range(12))

def randomDataString():
    distance = random.randint(20, 100)
    return ''.join(random.choice(string.ascii_uppercase) for i in range(distance))

def tests():
    initDB()
    latestKey = ""
    # Add to tree
    for i in range(1000):
        key = randomKeyString()
        Put(key, randomDataString())
        latestKey = key
    print latestKey, Get(latestKey)
    
def interactive():
    initDB()
    # print the options
    for opt, explanation in zip(OPTIONS, EXP_OPTIONS):
        print "%s - %s" %(opt, explanation)
    while True:
        # read an option form the commandline
        lnInput = raw_input("--: ")
        choice = lnInput.split(" ")[0]
        args = lnInput.split(" ")[1:]

        # place a value into the store
        if choice == 'p':
            Put(args[0], args[1])
        # get
        elif choice == 'g':
            print Get(args[0])
        # remove
        elif choice == 'r':
            Remove(args[0])
        # file choices added for convenience of dealing with very large data entries
        elif choice == 'pf':
            # open the file
            filenData = args[1]
            fData = open(filen, "rb")
            # read the data
            data = fData.read()
            #close the file
            fData.close()
            Put(args[0], data)
        elif choice == 'gf':
            data = Get(args[0])
            # open the file
            filenData = args[1]
            fData = open(filen, "wb")
            # write from db to file
            Data.write(data)
            #close the file
            fData.close()
                
        # choice to exit the value store program.
        elif choice == 'q':
            break
        # unknown input
        else: 
            continue

        #TODO for later parts - add to undo history


# Running the value store
if __name__ == "__main__":
    if len(sys.argv) > 1:
        interactive()
    else:
        tests()
