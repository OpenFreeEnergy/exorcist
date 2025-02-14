#!/bin/bash -x
initdb -D postgres_db
pg_ctl -D postgres_db -l postgres.log start
createdb exorcist_testdb
