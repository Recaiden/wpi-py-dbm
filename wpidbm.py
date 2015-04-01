#!/usr/bin/env python

import os       # file operations
import pickle   # serializing data structures
import fcntl    # exclusion of simultaneous edits.
import sys
import random
import string

import csv

from store import * # part 1
from btree import * # part 2

#db = None
#filenDB="./wpi.db"
#directory="wpi.db.locks"

COMP_LT = "LESS_THAN"
COMP_GT = "GREATER_THAN"
COMP_EQ = "EQUAL_TO"
COMP_LE = "LESS_THAN_OR_EQUAL_TO"
COMP_GE = "GREATER_THAN_OR_EQUAL_TO"

OPTIONS = ('p', 'g', 'r', 'pf', 'gf', 'q')
EXP_OPTIONS = ('Put: p key value',
               'Get: g key',
               'Remove: r key',
               'Put from file: pf key file',
               'Get to file: gf key file',
               'Quit')

# These 3 operations hold the file-access/refreshing and attach to the actual data-store.
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
    pass

def closeRelation(db, handle):
    pickle.dump(db, handle)
    handle.close()

def tplFrom(columns, comparators, values):
    '''
    Return all tuples in the relation that match a given criterion

    Each of the below are parallel lists
    compataror is less than, greater than, or equal
    value is the value being comnpared to
    column is the key into each record.
    '''

    openRelation()

    passed = []
    idx = 0
    valid = True
    
    while valid:
        tpl = getNext(idx)
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
        
    closeRelation()
    return passed

def select(member, column, comparator, value):
    '''Return all the entries in a given colum that match a given criterion'''
    tpls = tplFrom(column, comparator, value)
    vals = []
    for tpl in tpls:
        vals.append(tpl[member])
    return vals

#==============================================================================#
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

def csvs():
    # Create and populate city table
    schemaCity = ["Index", "City name", "Country Code", "State/Region", "Population"]

    if not os.path.exists("city.db"):
        cities = initDB("city", [])
        handle, cities = openRelation(cities)
        with open('city.csv', 'rb') as csvfile:
            cityreader = csv.reader(csvfile, delimiter=',')
            for row in cityreader:
                print ', '.join(row)
                entry = {}
                for item, attr in zip(row, schemaCity):
                    entry[attr] = item
                #Put(cities, row[0], entry)
                cities.insert(int(row[0]), entry)
        closeRelation(cities, handle)

    # create and populate country table
    schemaCountry = ["Country Code", "Country Name Alphabetized", "Continent",
                     "Region", "???01", "Date Established", "Population", "???02",
                     "???03", "???04", "Country Name Official", "Government Type",
                     "Government Leader", "???05", "Postal Code"]
    
    if not os.path.exists("country.db"):
        countries = initDB("country", [])
        with open('country.csv', 'rb') as csvfile:
            countryreader = csv.reader(csvfile, delimiter=',')
            for row in countryreader:
                print ', '.join(row)
                entry = {}
                for item, attr in zip(row, schemaCountry):
                    entry[attr] = item
                Put(countries, row[0], entry)
    
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
        csvs()
