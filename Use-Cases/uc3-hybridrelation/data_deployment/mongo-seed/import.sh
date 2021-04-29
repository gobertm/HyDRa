#! /bin/bash
mongoimport --host mongo --db mongo --collection ClientCollection --type json --jsonArray '/mongo-seed/ClientCollection.json'