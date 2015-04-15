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

def drift(years):
    for i in range(years):
        #TODO change population by 2% for each entry.
        # each year on each table will be a transaction
        pass
    pass

def replay(basename="clone"):
    #TODO
    pass

def display():
    #TODO
    pass

# def queryPopulation():
#     # Get the population of countries and find 40% of that
#     cntyNames= select(countries, "Country Code", ["Country Code"], [COMP_ALL], [0])
#     popcaps = {}
#     tpls = tplFrom(countries, ["Country Code"], [COMP_ALL], [0])
#     for country in tpls:
#         popcaps[country["Country Code"]] = int(country["Population"]) * 0.4

#     # For each country
#     #print cntyNames
#     for country in cntyNames:
#         # Select cities where country code matches
#         #and the population is greater than 40% of the country population
#         qualifiers = select(cities, "City name",
#                ["Country Code", "Population"],
#                [COMP_EQ, COMP_GT],
#                [country, popcaps[country]])
#         print country,
#         if len(qualifiers) == 0:
#             print "None"
#         else:
#             for city in qualifiers:
#                 print city,
#             print ""

def part4(years=1):
    clone("country", "clonecountry")
    clone("city", "clonecity")
    drift(years)
    replay()

    display()
    
# Running the value store
if __name__ == "__main__":
    if len(sys.argv) > 1:
        replay()
        
    part4()
