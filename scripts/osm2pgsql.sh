#!/bin/sh
osm2pgsql ~/demo/data/greater-london-latest.osm.pbf -d postgres -U postgres -P 5432 -H localhost -W
psql -d postgres -U postgres -a -f rename_columns.sql -h localhost

