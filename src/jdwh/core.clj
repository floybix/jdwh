(ns jdwh.core
  (:gen-class)
  (:require [clojure.java.jdbc :as sql]
            [clojure-csv.core :as csv]
            [clojure-ini.core :as ini]
            [clojure.string :as str]
            [clojure.java.io :as io])
  (:use     [clojure.tools.cli :only [cli]])
  (:import (java.sql SQLException PreparedStatement)))

(def debug (atom false))
(def timing (atom false))
(def echo (atom false))
(def keep-going (atom false))
(def have-log (atom false))

(defn msg
  "Print to *err*"
  [& args]
  (binding [*out* *err*]
    (apply println args)
    (flush)))

(defn get-db-config
  "Read database connection parameters from ~/.odbc.ini"
  [dbc-name]
  (let [user (System/getProperty "user.name")
        inifile (str "/home/" user "/.odbc.ini")
        conf0 (ini/read-ini inifile :comment-char \;
                            :allow-comments-anywhere? false)
        conf (zipmap (map str/lower-case (keys conf0))
                     (vals conf0))
        name-ok (str/lower-case dbc-name)]
    (let [sect (get conf name-ok {})
          dbc (sect "DBCName" "dbccop1")]
      (when (empty? sect)
        (msg "Section [" dbc-name "] not found in ~/.odbc.ini")
        (when @debug (msg conf))
        (System/exit 1))
      {:subprotocol "teradata"
       :classname "com.teradata.jdbc.TeraDriver"
       :subname (str "//" dbc "/")
       :user (sect "Username" user)
       :password (sect "Password")
       :CHARSET "UTF8"
       :USEXVIEWS "ON"})))

(defn strip-sql-comments
  "Remove comments from SQL code. It is very hard to do this in
   general because the comments might be embedded inside literal
   strings. Here we assume that '--' at the start of a line is a
   comment (not inside a string) and that all '/* */' blocks are
   comments (not inside a string). This will leave some '--' comments
   intact. We also assume that '--' followed on the same line by ';' is
   a comment. (This to avoid erroneously splitting sql statements)."
  [req]
  (-> req
      (str/replace #"(?s)/\*.*?\*/" "")
      (str/replace #"(?m)^\s*--.*" "")
      (str/replace #"(?m)--.*;.*" "")
      (str/replace #"(?s)[\n\r]{2,}+" "\n")))

(defn print-query-results
  "From a ResultSet print rows as CSV to *out* including
   column names in the first row. Returns number of rows.
   Does not keep the data in memory. This could be a simple use
   of (sql/with-query-results) but that uses hashmaps and so loses the
   order of columns.
   If `is-show` then the query is assumed to be a SHOW and we print the
   first value reformatted with newlines."
  [rset is-show]
  (let [rsmeta (.getMetaData rset)
        idxs (range 1 (inc (.getColumnCount rsmeta)))
        labs (mapv (fn [i] (.getColumnLabel rsmeta i)) idxs)
        ks (mapv keyword labs)
        rows (sql/resultset-seq rset)]
    (if is-show
      (let [row1col1 (ffirst rows)]
        (println (str/replace (val row1col1) #"\r" "\n")))
      (do
        (print (csv/write-csv [labs]))
        (doseq [row0 rows
                :let [row (mapv row0 ks)]]
          (print (csv/write-csv [(mapv str row)])))))
    (count rows)))

(defn process-warnings
  "Print any warnings from a PreparedStatement or Connection, and reset."
  [obj]
  (loop [w (.getWarnings obj)]
    (when w
      (msg "  *** WARNING:" (str w))
      (recur (.getNextWarning w))))
  (.clearWarnings obj))

(defn run-sql-request!
  "Execute a single or multiple-statement SQL request and print query
   results; result counts for each statment are printed to stderr.
   Repeats up to a given number of times on retryable errors."
  ([req]
     (run-sql-request! req 2))
  ([req retries]
     (when @echo (msg req "\n"))
     (let [start-time (. System (nanoTime))]
     (try
       (with-open [^PreparedStatement
                   stmt (sql/prepare-statement (sql/connection) req)]
         (when @debug (msg "DEBUG: checking connection warnings..."))
         (process-warnings (sql/connection))
         (when @debug (msg "DEBUG: checking prepared statement..."))
         (process-warnings stmt)
         (loop [has-results (.execute stmt)
                counts (.getUpdateCount stmt)
                is-show (re-seq #"(?si)^\s*show" req)]
           (if has-results
             (let [rows (print-query-results (.getResultSet stmt) is-show)]
               (msg "  *** Query successful," rows "rows returned."))
             ;; else (not a query)
             (when (not= counts -1)
               (msg "  *** Command successful,", counts "rows changed.")))
           ;; conditions indicating there may be more results:
           (when (or has-results (not= counts -1))
             (recur (.getMoreResults stmt) (.getUpdateCount stmt) is-show))))
       (catch SQLException e
         (when-not @echo
           (msg "  *** SQL statement causing exception: \n" req))
         ;; print exception at outer level in (-main) unless --keep-going.
         ;; also if writing to a log file, print it here too.
         (when (or @keep-going @have-log)
           (msg "  ***" (with-out-str (sql/print-sql-exception-chain e))))
         (if (and (= (.getSQLState e) "40001") ;; retryable
                  (pos? retries))
           (do
             (when-not @keep-going
               (msg "  ***" (with-out-str (sql/print-sql-exception-chain e))))
             (msg "  *** RETRYING;" retries "more attempts.")
             (Thread/sleep 1000)
             (run-sql-request! req (dec retries)))
           ;; non-retryable case:
           (when-not @keep-going (throw e))))
       (finally
         (when @timing
           (let [end-time (. System (nanoTime))]
             (msg (format "  *** Elapsed time: %.2f seconds"
                          (/ (- end-time start-time) 1e9))))))))))

(defn split-sql
  "Split SQL into statements on semicolons, but try to avoid splitting
   inside a string. Expects input with block comments already removed.
   Too hard to avoid strings in general, but
   we do avoid QUERY_BAND strings which use semicolons internally."
  [sql]
  (let [lines (str/split-lines sql)
        qb-re #"^\s*SET QUERY_BAND"
        qb-parts (partition-by #(nil? (re-seq qb-re %)) lines)
        subchunks (for [part qb-parts
                        :let [chunk (str/join "\n" part)]]
                    (if (re-seq qb-re chunk)
                      (list chunk)
                      (str/split chunk #";")))]
    (apply concat subchunks)))
    ;; TODO: do not split on semicolons preceded by backslash? ('foo\;')

(defn replace-tags
  "Replace embedded tags like <<DATABASENAME>> with environment
   variables of the same name. If the environment variable is unset,
   throw an exception. Prints a table of substitutions to stderr." 
  [sql]
  (let [found-tags (atom {})
        getenv (fn [[_ v]]
                 (when (re-seq #"\W" v)
                   (msg (str "ERROR: non-word characters in tag <<" v ">>"))
                   (throw (Exception. "non-word characters in tag")))
                 (let [val (System/getenv v)]
                   (when @debug (msg "DEBUG: tag" v "=" val))
                   (when (nil? val)
                     (msg (str "ERROR: no environment variable for tag <<" v ">>"))
                     (throw (Exception. "no environment variable for tag")))
                   (swap! found-tags assoc v val)
                   val))
        result (str/replace sql #"<<([^>]{1,32}+)>>" getenv)]
    ;; print out substituted values to stderr
    (doseq [[k v] @found-tags] (msg k "=" v))
    result))

(defn run-sql!
  "Execute SQL code consisting of one or more requests,
   splitting into individual statements. Argument tag?
   specifies embedded tag substitution."
  [sql tags? explain?]
  (sql/with-naming-strategy {:keyword identity}
    (let [stripped (strip-sql-comments sql)
          tagged (if tags? (replace-tags stripped) stripped)]
      (doseq [req (split-sql tagged)
              :when (not (str/blank? req))
              :let [req-to-run (str (if explain? "EXPLAIN " "") req)]]
        (run-sql-request! req-to-run)
        (flush)))))

(defmacro maybe-transaction
  "Wraps the body in (sql/transaction) if the predicate is true."
  [pred & body]
  `(if ~pred
     (sql/transaction ~@body)
     (do ~@body)))

(defmacro maybe-with-writer
  "If `out` is not nil, opens a writer on it bound to `bind`.
   Always evaluates the body."
  [out bind & body]
  `(if ~out
     (with-open [w# (io/writer ~out)]
       (binding [~bind w#]
         ~@body))
     (do ~@body)))

(defn- usage-intro []
  "
jdwh -- a command-line JDBC interface to DWH.

Usage:

With input from a file:
  jdwh -f SQLFILE [args]

With input inline:
  jdwh -c \"SELECT * FROM FOO\" [args]

With standard input:
  cat f1.sql f2.sql | jdwh [args]

Query results are written to stdout or --out as CSV.
")

(defn -main
  "Run with command-line arguments."
  [& args]
  (let [[opts extra banner]
        (cli args
             ["-c" "--command" "text of an SQL command to run."]
             ["-f" "--file" "SQL file to read; otherwise read Standard Input."]
             ["-o" "--out" "output file, default is standard out."]
             ["-l" "--log" "log file, default is stderr. If set, then --echo."]
             ["--tags" "replace tags like <<DATABASENAME>> from env vars."
              :flag true :default false]
             ["--transaction" "roll back everything if an exception occurs.",
              :flag true :default false]
             ["--explain" "just run explain on each statement, for checking.",
              :flag true :default false]
             ["--timing" "print elapsed time for each statement.",
              :flag true :default false]
             ["--fexp" "connect into fast export mode." :flag true :default false]
             ["--dbc" "name of configuration section in ~/.odbc.ini"
              :default "dwh32"]
             ["--encoding" "encoding of the input SQL."
              :default "UTF-8"]
             ["-e" "--echo" "echo all SQL commands sent to server to stderr."
              :flag true :default nil]
             ["-d" "--debug" "print detailed output." 
              :flag true :default false]
             ["-k" "--keep-going" "continue even if sql statements fail." 
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
    (reset! echo (:echo opts))
    (reset! debug (:debug opts))
    (reset! timing (:timing opts))
    (reset! keep-going (:keep-going opts))
    (reset! have-log (:log opts))
    (let [{:keys [command file out log
                  tags fexp dbc explain
                  encoding]} opts
          do-tx? (:transaction opts)
          dbinfo (assoc (get-db-config dbc)
                   :TYPE (if fexp "FASTEXPORT" "DEFAULT"))
          have-f (not (or (str/blank? file) (= file "-")))
          stdin (when (and (not have-f) (str/blank? command))
                  (msg "  *** Reading SQL from Standard Input...")
                  (slurp *in* :encoding encoding))
          filein (when have-f
                   (slurp file :encoding encoding))
          sql-str (str command stdin filein)]
      (when log
        ;; echo defaults to true if log is set
        (when (nil? @echo) (reset! echo true)))
      (when @debug
        (msg "DEBUG: opts =" opts)
        (msg "DEBUG: config =" (dissoc dbinfo :password))
        (msg "DEBUG: input SQL =" sql-str))
      (when (str/blank? sql-str)
        (msg "No SQL given.")
        (System/exit 1))
      (try
        (maybe-with-writer out *out*
          (maybe-with-writer log *err*
            (sql/with-connection dbinfo
              (maybe-transaction do-tx?
                 (run-sql! sql-str tags explain)))))
        (catch SQLException e
          (msg "  ***" (with-out-str (sql/print-sql-exception e)))
          (System/exit 1))))))
