(ns jdbcrecon.core
  (:require [clojure.tools.logging :as log])
  (:require [clojure.java.jdbc :as sql]))

(defn build-query
  "Uses the params to build the select query. 
  Expects to have :tblname, :keycols, :versioncol, and optional :querysuffix"
  [params]
  (.trim (str "SELECT " 
           (:keycols params)
           ","
           (:versioncol params) 
           " FROM " 
           (:tblname params) 
           " "
           (:querysuffix params))))

(defn build-entity
  "Converts a result set entry into the entity expected by the recon and exception
  functions based on the params specified"
  [params row]
  (vector 
    (reduce 
      #(assoc %1 %2 (get row (keyword %2))) 
      {} 
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

(comment
(defn entity-seq
  "Queries a data source and returns a sequence [{k1 v1 k2 v2 ...} version]"
  [params]
  (let [query (build-query params)]
    (log/debug "Executing query: " query)
    (sql/with-connection params
      (sql/with-query-results rs [query]
        ; TODO: result set gets closed when returning
        (map #(build-entity params %1) rs)))))
)

(defn reconcile
  "Executes a reconciliation.  source-params and target-params includes all connection parameters 
  required by clojure.contrib.sql with-connection as well as the following:
  :tblname <name>
  :keycols [seq of names]
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

;  (let [src-seq (entity-seq source-params)
;        tgt-seq (entity-seq target-params)]
;    (doseq [e (recon-func src-seq tgt-seq)]
;      (exception-func e))))
