(ns jdwh.put
  (:gen-class)
  (:require [clojure.java.jdbc :as sql]
            [clojure-csv.core :as csv]
            [clojure-ini.core :as ini]
            [clojure.string :as str]
            [clojure.java.io :as io])
  (:use     [clojure.tools.cli :only [cli]]
            [jdwh.core :exclude [-main]])
  (:import (java.sql SQLException BatchUpdateException PreparedStatement)))

(defn run-load!
  "Load from a CSV file into a table using a batched insert statement."
  [table in-csv encoding batch-size]
  (try
    (with-open [rdr (io/reader in-csv :encoding encoding)]
      (when @debug (msg "DEBUG: batch size =" batch-size))
      (let [rows (rest (csv/parse-csv rdr))
            batches (partition batch-size batch-size nil rows)]
        (doseq [batch batches
                :let [upds (apply sql/insert-rows table batch)
                      cnt (count (filter #(= 1 %) upds))]]
          (msg "  *** Command successful," cnt "out of" (count upds)
               "rows inserted."))))
    (catch BatchUpdateException e
      ;; there is a chain of exceptions, one for each row!
      (msg "  ***" (with-out-str (sql/print-sql-exception e)))
      (msg "  ***" (with-out-str (sql/print-sql-exception (.getNextException e))))
      (throw e))
    (catch SQLException e
      ;; print exception at outer level in (-main), but
      ;; if writing to a log file, print it here too.
      (msg "  ***" (with-out-str (sql/print-sql-exception-chain e)))
      (throw e)))
  (when @debug (msg "DEBUG: checking connection warnings..."))
  (process-warnings (sql/connection)))

(defn- usage-intro []
  "
jdwhput -- a command-line JDBC uploader to DWH.

Usage:

  jdwhput -i INPUT.CSV --table PINFO.FOO [args]

")

(defn -main
  "Run with command-line arguments."
  [& args]
  (let [[opts extra banner]
        (cli args
             ["-i" "--in-csv" "input CSV file to load into a table."]
             ["--table" "target table including database: 'database.table'"] 
             ["-l" "--log" "log file, default is stderr."]
             ["--transaction" "roll back inserts if an exception occurs.",
              :flag true :default false]
             ["--fastload" "use FASTLOAD connection: empty table, >100k rows."
              :flag true :default false]
             ["--dbc" "name of configuration section in ~/.odbc.ini"
              :default "dwh32"]
             ["--encoding" "encoding of the input file."
              :default "UTF-8"]
             ["-d" "--debug" "print detailed output." 
              :flag true :default false]
             ["-h" "--help" "print this help message."
              :flag true :default false])
        usage (str (usage-intro)
                   (str/replace banner #"Usage:" ""))]
    (when (:help opts)
      (println usage)
      (System/exit 0))
    (when-not (empty? extra)
      (msg "Error: unmatched command-line arguments: " extra)
      (System/exit 1))
    (when-not (:in-csv opts)
      (msg "Error: --in-csv is a required argument.")
      (System/exit 1))
    (when-not (:table opts)
      (msg "Error: --table is a required argument.")
      (System/exit 1))
    (reset! debug (:debug opts))
    (reset! have-log (:log opts))
    (let [{:keys [in-csv table fastload
                  log dbc encoding]} opts
          ;; need a transaction with fastload because of batching:
          ;; can not fastload into non-empty table
          do-tx? (or (:transaction opts) fastload)
          dbinfo (assoc (get-db-config dbc)
                   :TYPE (if fastload "FASTLOAD" "DEFAULT"))
          batch (if fastload 80000 10000)]
      (when @debug
        (msg "DEBUG: opts =" opts)
        (msg "DEBUG: config =" (dissoc dbinfo :password))
        (msg "DEBUG: target table =" table))
      (try
        (maybe-with-writer log *err*
          (sql/with-connection dbinfo
            (maybe-transaction do-tx?
               (run-load! table in-csv encoding batch))))
        (catch BatchUpdateException e
          (System/exit 1))
        (catch SQLException e
          (msg "  ***" (with-out-str (sql/print-sql-exception-chain e)))
          (System/exit 1))))))
