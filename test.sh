#!/bin/bash

## for tags:
export DATABASENAME=pinfo
export START_TIMESTAMP="2010-12-31 12:34:56"

for x in test-sql/*.sql; do
    echo "___ running $x ___"
    bash target/jdwh --tags -f $x -e >$x.out 2>&1
    if [ -f $x.out.ref ]; then
	diff $x.out.ref $x.out
    fi
done
