#!/usr/bin/env python

import os       # file operations
import pickle   # serializing data structures
import fcntl    # exclusion of simultaneous edits.

FILEN_EXT__LOCK = ".lock"
LOCK_WRITE = "lock"

class store(object):
    
    def __init__(self, filenDB="./wpi.db"):
        '''Opens the database file, creating it if it doesn't exist, and initializes and 
        options and settings for this session, should those exist later'''
        self.db = {}
        self.dbopen(filenDB)
        try:
            with open(self.filenDB, 'r+b') as handle:
                self.db = pickle.load(handle)
        except EOFError: print "no db yet."
        
    def dbopen(self, filenDB):
        ''''''
        self.filenDB = filenDB
        if os.path.exists(filenDB):
            pass # open DB
        else:
            f = open(self.filenDB, "w")
            f.close()
        return True

    def put(self, key, data):
        '''stores data under the given key.'''

        # acquire lock and open files
        handle = open(self.filenDB, 'w+b')
        fcntl.lockf(handle, fcntl.LOCK_EX)
        marker = open(".%s"%key, 'w+b')
        fcntl.lockf(marker, fcntl.LOCK_EX)

        # Update store from file, set new data, rewrite store to file
        self.db = pickle.load(handle)
        self.db[key] = data
        pickle.dump(self.db, handle)

        # release lock and clean up
        fcntl.lockf(marker, fcntl.LOCK_UN)
        fcntl.lockf(handle, fcntl.LOCK_UN)
        marker.close()
        handle.close()

        return True

    def get(self, key):
        '''retrieves the data.'''

        # acquire lock and open files
        marker = open(".%s"%key, 'w+b')
        fcntl.lockf(marker, fcntl.LOCK_SH)

        handle = open(self.filenDB, 'rb')
        self.db = pickle.load(handle)
        handle.close()
        fcntl.lockf(marker, fcntl.LOCK_UN)
        marker.close()

        retval = None
        try:
            retval = self.db[key]
        except KeyError:
            retval = "ERROR: key not found"
        return retval
        

    def remove(self, key):
        '''deletes the key.'''

        # acquire lock and open files
        handle = open(self.filenDB, 'w+b')
        fcntl.lockf(handle, fcntl.LOCK_EX)
        marker = open(".%s"%key, 'w+b')
        fcntl.lockf(marker, fcntl.LOCK_EX)

        # Update store from file, set new data, rewrite store to file
        self.db = pickle.load(handle)
        present = self.db.pop(key, None)
        pickle.dump(self.db, handle)

        # release lock and clean up
        fcntl.lockf(marker, fcntl.LOCK_UN)
        fcntl.lockf(handle, fcntl.LOCK_UN)
        marker.close()
        handle.close()
        
        if present == None:
            return "ERROR: key not found"
        else:
            return True
