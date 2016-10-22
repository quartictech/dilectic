#!/bin/bash
set -eu

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/config.sh

osm2pgsql $INPUT/greater-london-latest.osm.pbf -d postgres -U postgres -P 5432 -H localhost
psql -d postgres -U postgres -a -f $DIR/scripts/rename_columns.sql -h localhost
