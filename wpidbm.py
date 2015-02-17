#!/usr/bin/env python

import os       # file operations
import pickle   # serializing data structures
#import shelf    # automated persistence of dictionary-like structures

FILEN_EXT__LOCK = ".lock"

db = None

OPTIONS = ('p', 'g', 'r', 'pf', 'gf', 'q')
EXP_OPTIONS = ('Put: p key value',
               'Get: g key',
               'Remove: r key',
               'Put from file: pf key file',
               'Get to file: gf key file',
               'Quit')

class store(object):
    
    def __init__(self, filenDB="./wpi.db"):
        '''Opens the database file, creating it if it doesn't exist, and initializes and 
        options and settings for this session, should those exist later'''
        self.dbopen(filenDB)
        
        
    def dbopen(self, filenDB):
        ''''''
        self.filenDB = filenDB
        if os.path.exists(filenDB):
            pass # open DB
        else:
            pass # create DB
        return True

    def put(self, key, data):
        '''stores data under the given key.'''
        self.db[key] = data
        #TODO check that not in use?
        with open(self.filenDB, 'wb') as handle:
            pickle.dump(self.db, handle)
        return True

    def get(self, key):
        '''retrieves the data.'''

        #TODO check that not in use?
        with open(self.filenDB, 'rb') as handle:
            self.db = pickle.load(handle)
        return self.db[key]

    def remove(self, key):
        '''deletes the key.'''

        present = self.db.pop(key, None)
        with open(self.filenDB, 'wb') as handle:
            pickle.dump(self.db, handle)
        
        if present == None:
            return "Key not found"
        else:
            return True
        
    
def Put(key, data):
    '''stores data under the given key.'''
    return db.put(kay, data)
def Get(key):
    '''retrieves the data.'''
    return db.get(key)
def Remove(key):
    '''deletes the key.'''
    return db.remove(key)


# Running the value store
if __name__ == "__main__":
    db = store()
    # print the options
    for opt, explanation in zip(OPTIONS, EXP_OPTIONS):
        print "%s - %s\n" %(opt, explanation)
    while True:
        # read an option form the commandline
        lnInput = raw_input()
        choice = lnInput.split(" ")[0]
        args = lnInput.split(" ")[1:]

        # place a value into the store
        if choice == 'p':
            Put(args[0], args[1])
        # get
        elif choice == 'g':
            Get(args[0])
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
