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
    shutil.copy2("%s.db", "%s.db" %(current, new))
    shutil.copy2("%s.db.log", "%s.db.log" %(current, new))
    shutil.copytree("%s.db.locks", "%s.db.log" %(current, new))

    # Load the db and change its internal pathnames
    handle = open("%s.db" %current, 'wb')
    db = pickle.load(handle)
    db.path = "%s.db" %new
    db.pathdir = "%s.db.locks" %new
    db.pathlog = "%s.db.log" %new
    pickle.dump(db, handle)
    handle.close()

def transferLogs(current, new):
    shutil.copy2("%s.db.log", "%s.db.log" %(current, new))

def drift(years):
    cities = initDB("city", [])
    countries = initDB("country", [])
    
    for i in range(years):
        # each year on each table will be a transaction
        countries.TBegin()
        handle, countries = openRelation(countries)
        # Get all countries
        tpls = tplFrom(countries, ["Country Code"], [COMP_ALL], [0])
        for country in tpls:
            popNew = int(int(country["Population"]) * 1.02)
            ctryNew = country
            ctryNew["Population"] = popNew
            countries.TRecord(country["Country Code"], country, ctryNew)
            countries.remove(country["Country Code"])
            countries.insert(country["Country Code"], ctryNew)
        countries.TCommit()
        closeRelation(countries)

        cities.TBegin()
        handle, cities = openRelation(cities)
        tpls = tplFrom(cities, ["Index"], [COMP_ALL], [0])
        for city in tpls:
            popNew = int(int(city["Population"]) * 1.02)
            ctryNew = city
            ctryNew["Population"] = popNew
            cities.TRecord(city["Index"], city, ctryNew)
            cities.remove(city["Index"])
            cities.insert(city["Index"], cctryNew)
        cities.TCommit()
        closeRelation(cities)

def replay(table, basename="clone"):
    db = initDB("%s%s"%(basename, table), [])
    log = open(db.pathlog, 'rb')
    handle = None
    for line in log:
        # Start
        if line.split("|")[1] == "START":
            handle, db = openRelation(db)
            
        # Commit
        elif line.split("|")[1] == "COMMIT":
            closeRelation(db, handle)
            
        # Insertion
        elif line.split("|")[2] == "NULL":
            db.insert(line.split("|")[1], line.split("|")[3])
        
        # Deletion
        elif line.split("|")[3] == "NULL":
            # Possibly check for existence?
            db.remove(line.split("|")[1])
        
        # Update
        else:
            # Possibly check matching of previous value?
            db.remove(line.split("|")[1])
            db.insert(line.split("|")[1], line.split("|")[3])

def display():
    cities = initDB("city", [])
    countries = initDB("country", [])
    
    handle, countries = openRelation(countries)
    tpls = tplFrom(countries, ["Country Code"], [COMP_ALL], [0])
    for country in tpls:
        print country["Country Name Official"], country["Population"]
    closeRelation(countries)

    handle, cities = openRelation(cities)
    tpls = tplFrom(cities, ["Country Code"], [COMP_ALL], [0])
    for city in tpls:
        print city["City name"], city["Population"]
    closeRelation(cities)


def part4(years=1):
    clone("country", "clonecountry")
    clone("city", "clonecity")
    drift(years)
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
