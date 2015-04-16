#!/usr/bin/env python

from store import * # part 1
from btree import * # part 2
from dbmglobals import *

import sys
import shutil # copying files

    #cities = initDB("city", [])
    #countries = initDB("country", [])

def clone(current, new):
    '''Current is the old root of the db, ie 'city'
    new is the location being copied to, ie 'clonecity' '''
    # Copy the files
    shutil.copy2("%s.db"%current, "%s.db" %new)
    shutil.copy2("%s.db.log"%current, "%s.db.log" %new)
    shutil.copytree("%s.db.locks"%current, "%s.db.locks" %new)

    # Load the db and change its internal pathnames
    handle = open("%s.db"%new, 'rb')
    db = pickle.load(handle)
    handle.close()
    handle = open("%s.db"%new, 'w+b')
    
    db.path = "%s.db" %new
    db.pathdir = "%s.db.locks" %new
    db.pathlog = "%s.db.log" %new
    pickle.dump(db, handle)
    handle.close()

def transferLogs(current, new):
    shutil.copy2("%s.db.log"%current, "%s.db.log" %new)

def drift(years):
    cities = initDB("city", [])
    countries = initDB("country", [])
    #print countries.path, countries.transaction
    
    for i in range(years):
        # each year on each table will be a transaction
        countries.TBegin()
        #handle, countries = openRelation(countries)
        # Get all countries
        tpls = tplFrom(countries, ["Country Code"], [COMP_ALL], [0])
        for country in tpls:
            popNew = int(int(country["Population"]) * 1.02)
            ctryNew = country
            ctryNew["Population"] = popNew
            countries.TRecord(country["Country Code"], country, ctryNew)
            #countries.remove(country["Country Code"])
            countries.insert(country["Country Code"], ctryNew)
        countries.TCommit()

        handle = open(countries.path, 'w+b')
        pickle.dump(countries, handle)
        handle.close()

        cities.TBegin()
        #handle, cities = openRelation(cities)
        tpls = tplFrom(cities, ["Index"], [COMP_ALL], [0])
        for city in tpls:
            popNew = int(int(city["Population"]) * 1.02)
            ctyNew = city
            ctyNew["Population"] = popNew
            cities.TRecord(city["Index"], city, ctyNew)
            #cities.remove(int(city["Index"]))
            cities.insert(int(city["Index"]), ctyNew)
        cities.TCommit() 
        
def replay(table, basename="clone"):
    db = initDB("%s%s"%(basename, table), [])
    log = open(db.pathlog, 'rb')

    for line in log:
        # Start
        if line.strip("\r\n").split("|")[1] == "START":
            #handle, db = openRelation(db)
            pass
            
        # Commit
        elif line.strip("\r\n").split("|")[1] == "COMMIT":
            #closeRelation(db, handle)
            pass
            
        # Insertion
        elif line.strip("\r\n").split("|")[2] == "NULL":
            key = line.strip("\r\n").split("|")[1]
            valNew = eval(line.strip("\r\n").split("|")[3])
            if db.get(key) != valNew:
                db[key] = valNew
        
        # Deletion
        elif line.strip("\r\n").split("|")[3] == "NULL":
            # Possibly check for existence?
            #db.remove(line.split("|")[1])
            db.insert(line.strip("\r\n").split("|")[1],
                      eval(line.strip("\r\n").split("|")[3]))
        
        # Update
        else:
            # Possibly check matching of previous value?
            key = line.strip("\r\n").split("|")[1]
            valNew = eval(line.strip("\r\n").split("|")[3])
            if db.get(key) != valNew:
                db[key] = valNew
            
    handle = open(db.path, 'w+b')
    pickle.dump(db, handle)
    handle.close()

def display():
    cities = initDB("clonecity", [])
    countries = initDB("clonecountry", [])
    
    tpls = tplFrom(countries, ["Country Code"], [COMP_ALL], [0])
    for country in tpls:
        print country["Country Name Official"], country["Population"]
    
     tpls = tplFrom(cities, ["Country Code"], [COMP_ALL], [0])
     for city in tpls:
         print city["City name"], city["Population"]
    

def part4(years=1):
    sys.setrecursionlimit(10000)
    clone("country", "clonecountry")
    clone("city", "clonecity")
    drift(years)
    transferLogs("country", "clonecountry")
    transferLogs("city", "clonecity")
    replay("city")
    replay("country")

    display()
    
# Running the value store
if __name__ == "__main__":
    if len(sys.argv) > 1:
        replay("city")
        replay("country")
    else:
        part4()
