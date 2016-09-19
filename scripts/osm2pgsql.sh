#!/bin/sh
osm2pgsql ../data/greater-london-latest.osm.pbf -d postgres -U postgres -P 5432
psql -d postgres -U postgres -a -f rename_columns.sql
