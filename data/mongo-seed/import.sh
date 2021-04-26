#! /bin/bash
mongoimport --host mymongo --db mymongo --collection actorCollection --type json --jsonArray '/mongo-seed/actorCollection.json'