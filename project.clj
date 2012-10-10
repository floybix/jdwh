(defproject jdwh "0.7.1"
  :description "Command-line interface to the DWH via JDBC"
  :url "https://github.com/floybix/jdwh"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/java.jdbc "0.2.3"]
                 [com.teradata.jdbc/TeraDriver "14.00.00.14"]
                 [com.teradata.jdbc/tdgssconfig "14.00.00.14"]
                 [clojure-csv/clojure-csv "2.0.0-alpha1"]
                 [clojure-ini "0.0.1"]
                 [org.clojure/tools.cli "0.2.1"]
                 [org.clojure/clojure "1.4.0"]]
  :main jdwh.core
  :aot [jdwh.core jdwh.put])
