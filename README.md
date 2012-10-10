# jdwh

A command-line interface to the Teradata DWH:
  * fast and simple execution of SQL
  * downloading and uploading of data in CSV format
  * substitution of variables in SQL

Written in Clojure, using JDBC, compiles to an executable JAR.

http://github.com/floybix/jdwh

Could be adapted to other databases without much effort.

## Installation

The easy option is to download the compiled executable jars from the
downloads page. These can be placed on your $PATH on unix-like systems
or run as Java jars.

## Compiling

To compile you will need the clojure build tool
(Leiningen)[http://leiningen.org/] v2 and the `lein-localrepo` plugin.

You will need to download the Teradata JDBC driver v 14.00.00.14 from
http://downloads.teradata.com/download/connectivity/jdbc-driver

Install the JDBC driver (two JAR files) for Leiningen as follows:

    lein localrepo install terajdbc4.jar com.teradata.jdbc/TeraDriver 14.00.00.14
    lein localrepo install tdgssconfig.jar com.teradata.jdbc/tdgssconfig 14.00.00.14

Then to compile:

    make jar

This will generate an executable jar at `target/jdwh` and an
associated shell script at `target/jdwhput`. They can then be
installed to e.g. `/usr/local/bin/`

### Running

There are two commands: `jdwh` and `jdwhput` (for uploading). They can
be run locally with

    lein run --help
    lein run -m jdwh.put --help

or, after compiling:

    java -jar target/jdwh-*-standalone.jar --help
    java -cp target/jdwh-*-standalone.jar jdwh.put --help

or

    target/jdwh --help
    target/jdwhput --help


## jdwh Usage

```
jdwh -- a command-line JDBC interface to DWH.

Usage:

With input from a file:
  jdwh -f SQLFILE [args]

With input inline:
  jdwh -c "SELECT * FROM FOO" [args]

With standard input:
  cat f1.sql f2.sql | jdwh [args]

Query results are written to stdout or --out as CSV.


 Switches                           Default  Desc                                              
 --------                           -------  ----                                              
 -c, --command                               text of an SQL command to run.                    
 -f, --file                                  SQL file to read; otherwise read Standard Input.  
 -o, --out                                   output file, default is standard out.             
 -l, --log                                   log file, default is stderr. If set, then --echo. 
 --no-tags, --tags                  false    replace tags like <<DATABASENAME>> from env vars. 
 --no-transaction, --transaction    false    roll back everything if an exception occurs.      
 --no-explain, --explain            false    just run explain on each statement, for checking. 
 --no-timing, --timing              false    print elapsed time for each statement.            
 --no-fexp, --fexp                  false    connect into fast export mode.                    
 --dbc                              dwh32    name of configuration section in ~/.odbc.ini      
 --encoding                         UTF-8    encoding of the input SQL.                        
 -e, --no-echo, --echo                       echo all SQL commands sent to server to stderr.   
 -d, --no-debug, --debug            false    print detailed output.                            
 -k, --no-keep-going, --keep-going  false    continue even if sql statements fail.             
 -h, --no-help, --help              false    print this help message.                          
```

## jdwhput Usage

```
jdwhput -- a command-line JDBC uploader to DWH.

Usage:

  jdwhput -i INPUT.CSV --table PINFO.FOO [args]



 Switches                         Default  Desc                                              
 --------                         -------  ----                                              
 -i, --in-csv                              input CSV file to load into a table.              
 --table                                   target table including database: 'database.table' 
 -l, --log                                 log file, default is stderr.                      
 --no-transaction, --transaction  false    roll back inserts if an exception occurs.         
 --no-fastload, --fastload        false    use FASTLOAD connection: empty table, >100k rows. 
 --dbc                            dwh32    name of configuration section in ~/.odbc.ini      
 --encoding                       UTF-8    encoding of the input file.                       
 -d, --no-debug, --debug          false    print detailed output.                            
 -h, --no-help, --help            false    print this help message.                          
```


## Database Authentication

Authentication details are read from `~/.odbc.ini`. For example, your
~/.odbc.ini might contain the following:

    [dwh32]
    DBCName=dbc
    Username=USERID
    Password=PASSWORD

By default, it looks for a section called [dwh32]. You can specify a
different section from your ~/.odbc.ini with the --dbc command-line
option.


## Examples

...

### Bugs

...


## License

Copyright © 2012 Felix Andrews, Australian Taxation Office,
Commonwealth of Australia.

Distributed under the Eclipse Public License, the same as Clojure.
