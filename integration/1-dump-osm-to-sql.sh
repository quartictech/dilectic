#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/config.sh

osm2pgsql $INPUT/greater-london-latest.osm.pbf -d postgres -U postgres -P 5432 -H mdillon__postgis
psql -d postgres -U postgres -a -f $DIR/scripts/rename_columns.sql -h mdillon__postgis
