#!/usr/bin/env python

from store import * # part 1
from btree import * # part 2
from dbmglobals import *

import csv

def csvs():
    # Create and populate city table
    schemaCity = ["Index", "City name", "Country Code", "State/Region", "Population"]
    if not os.path.exists("city.db"):
        cities = initDB("city", [])
        handle, cities = openRelation(cities)
        cities.TBegin()
        with open('city.csv', 'rb') as csvfile:
            cityreader = csv.reader(csvfile, delimiter=',')
            for row in cityreader:
                #print ', '.join(row)
                entry = {}
                for item, attr in zip(row, schemaCity):
                    entry[attr] = item
                cities.TRecord(int(row[0]), "NULL", entry)
                cities.insert(int(row[0]), entry)
        cities.TCommit()
        closeRelation(cities, handle)

    # create and populate country table
    schemaCountry = ["Country Code", "Country Name Alphabetized", "Continent",
                     "Region", "???01", "Date Established", "Population", "???02",
                     "???03", "???04", "Country Name Official", "Government Type",
                     "Government Leader", "???05", "Postal Code"]
    
    if not os.path.exists("country.db"):
        countries = initDB("country", [])
        handle, countries = openRelation(countries)
        countries.TBegin()
        with open('country.csv', 'rb') as csvfile:
            countryreader = csv.reader(csvfile, delimiter=',')
            for row in countryreader:
                #print ', '.join(row)
                entry = {}
                for item, attr in zip(row, schemaCountry):
                    entry[attr] = item
                countries.TRecord(row[0], "NULL", entry)
                countries.insert(row[0], entry)
        countries.TCommit()
        closeRelation(countries, handle)

def queryPopulation():
    # Get the population of countries and find 40% of that
    cities = initDB("city", [])
    countries = initDB("country", [])
    
    cntyNames= select(countries, "Country Code", ["Country Code"], [COMP_ALL], [0])
    popcaps = {}
    tpls = tplFrom(countries, ["Country Code"], [COMP_ALL], [0])
    for country in tpls:
        popcaps[country["Country Code"]] = int(country["Population"]) * 0.4

    # For each country
    #print cntyNames
    for country in cntyNames:
        # Select cities where country code matches
        #and the population is greater than 40% of the country population
        qualifiers = select(cities, "City name",
               ["Country Code", "Population"],
               [COMP_EQ, COMP_GT],
               [country, popcaps[country]])
        print country,
        if len(qualifiers) == 0:
            print "None"
        else:
            for city in qualifiers:
                print city,
            print ""

def part3():
    cities, countries = csvs()
    queryPopulation()

# Running the value store
if __name__ == "__main__":
    part3()
