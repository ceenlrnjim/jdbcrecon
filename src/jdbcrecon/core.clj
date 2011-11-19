(ns jdbcrecon.core
  (:require [clojure.tools.logging :as log])
  (:require [clojure.java.jdbc :as sql]))

(defn build-query
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

(defn build-entity
  "Converts a result set entry into the entity expected by the recon and exception
  functions based on the params specified"
  [params row]
  (vector 
    (reduce 
      ; Note that column names must be in the same order in the configuration
      #(conj %1 (get row (keyword %2))) 
      [] 
      (.split (:keycols params) ",")) 
    (get row (keyword (:versioncol params)))))

(defn entity-key
  "Takes an entity from a entity-seq returns the map of its key column names and values"
  [e]
  (e 0))

(defn entity-version
  "Takes an entity from a entity-seq returns the map of its version"
  [e]
  (e 1))

(defn reconcile
  "Executes a reconciliation.  source-params and target-params includes all connection parameters 
  required by clojure.contrib.sql with-connection as well as the following:
  :tblname <name>
  :keycols [seq of names] - note that the keycols must be in the same order in both configs though the names can be different
  :versioncol <name>
  :touchcol <name> This can be nil for targets
  :touchval <value> the value to be set when touching records
  
  Two different compare types are supported, :version and :timestamp
  
  Recon-func should take two sequences of entities.
  Exception-func should take a sequence of [key-map issue]"
  [source-params target-params recon-func compare-type exception-func]
  (sql/with-connection source-params
    (sql/with-query-results src-rs [(build-query source-params)]
      (sql/with-connection target-params
        (sql/with-query-results tgt-rs [(build-query target-params)]
          (doseq [e (recon-func
                      (map #(build-entity source-params %) src-rs)
                      (map #(build-entity target-params %) tgt-rs))]
            (exception-func e)))))))
