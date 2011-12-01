(ns jdbcrecon.core
  (:require [clojure.tools.logging :as log])
  (:require [clojure.java.jdbc :as sql]))

(defn- build-query
  "Uses the params to build the select query. 
  Expects to have :tblname, :keycols, :versioncol, and optional :querysuffix"
  [params]
  (let [result (.trim (str "SELECT " 
                         (:keycols params)
                         ","
                         (:versioncol params) 
                         " FROM " 
                         (:tblname params) 
                         " "
                         (:querysuffix params)))]
    (log/debug "Query = " result)
    result))

(defn- build-record
  "Converts a result set row into the record expected by the recon and exception
  functions based on the params specified.  Returns a data structure containing a key 
  (accessible with the record-key accessor) and a version (via the record-version accessor)"
  [params row]
  (vector 
    (reduce 
      ; Note that column names must be in the same order in the configuration
      #(conj %1 (get row (keyword %2))) 
      [] 
      (.split (:keycols params) ",")) 
    (get row (keyword (:versioncol params)))))

(defn record-key
  "Accessor that returns the key for a record"
  [e]
  (e 0))

(defn record-version
  "accessor that reutns the version for a record"
  [e]
  (e 1))

; TODO: need the structure of exceptions to be defined by the core, not implicit in the various recon alogrithms

(defn reconcile
  "Executes a reconciliation.  source-params and target-params includes all connection parameters 
  required by clojure.contrib.sql with-connection as well as the following:
  :tblname <name>
  :keycols [seq of names] - note that the keycols must be in the same order in both configs though the names can be different
  :versioncol <name>
  :touchcol <name> This can be nil for targets
  :touchval <value> the value to be set when touching records
  
  Two different compare types are supported, :version and :timestamp
  
  Recon-func should be a function that takes two lazy sequences of records (see record-key and record-version)
  and returns a sequence of exceptions (vector with keys at index 0 and issue code at index 1).

  Exception-func should take a sequence of [key-seq issue] and responds accordingly with some side effect"
  [source-params target-params recon-func compare-type exception-func]
  (sql/with-connection source-params
    (sql/with-query-results src-rs [(build-query source-params)]
      (sql/with-connection target-params
        (sql/with-query-results tgt-rs [(build-query target-params)]
          (doseq [e (recon-func
                      (map #(build-record source-params %) src-rs)
                      (map #(build-record target-params %) tgt-rs))]
            (exception-func e)))))))
