#! /bin/bash
mongorestore --host mymongo --port 27000 -d mymongo -c movies /data/movies.bson
mongorestore --host mymongo --port 27000 -d mymongo -c comments /data/comments.bson