#!/bin/bash

## rewrite the command to strip off last 3 chars: jdwhput -> jdwh
JDWH=`echo $0 | head -c -4`
exec java -cp ${JDWH} jdwh.put "$@"
