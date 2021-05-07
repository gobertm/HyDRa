#! /bin/bash
mongoimport --host mymongo2 --db mymongo2 --collection categoryCollection --type json --jsonArray '/mongo-seed/categoryCollection.json'