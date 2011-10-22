(ns jdbcrecon.core
  (:require [clojure.java.jdbc :as sql]))

(defn build-query
  "Uses the params to build the select query. 
  Expects to have :tblname, :keycols, :versioncol, and optional :querysuffix"
  [params]
  (.trim (str "SELECT " 
           (reduce #(str %1 "," %2) (:keycols params)) 
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
      (:keycols params)) 
    (get row (keyword (:versioncol params)))))

(defn- entity-seq
  "Queries a data source and returns a sequence [{k1 v1 k2 v2 ...} version]"
  [params]
  (let [query (build-query params)]
    (sql/with-connection params
      (sql/with-query-results rs [query]
        (map #(build-entity params %1) rs)))))

; TODO: order compare recon
; TODO: memory-map compare recon
; TODO: touch entity
; TODO: exception-func implementations (log, jms, touch, system out)

(defn tgt-behind?
  "Returns true if the src row is ahead of the target row"
  [src-row tgt-row]
  (> (src-row 1) (tgt-row 1)))

(defn src-behind?
  "Returns true if the target row is ahead of the src row"
  [src-row tgt-row]
  (< (src-row 1) (tgt-row 1)))

(defn issue-code
  "Returns the issue keyword for the specified rows or nil if there is no issue"
  [src-row tgt-row]
  (cond
    (tgt-behind? src-row tgt-row) :tgt-behind
    (src-behind? src-row tgt-row) :src-behind
    :else nil))

(defn no-nil-cons
  "Adds item to seq if it is not nil, otherwise returns seq"
  [item seq]
  (if (nil? item) seq (cons item seq)))

(defn ordered-row-recon
  "Reconciliation function that assumes the two sequences share the same ordering.
  Reconciliation is applied as the sequences are consumed.  Useful where the tables are larger
  and we don't want to load all the contents in memory as with other implementations.
  Returns a sequence of [keymap <issue>] where issue is :tgt-behind :src-behind :tgt-missing :src-missing"
  [src-seq tgt-seq]
  ; start off assuming nothing can be missing - just checking versions
  ; and both sequences are same size
  (let [src-row (first src-seq)
        tgt-row (first tgt-seq)]
    (if (empty? src-seq) nil 
      (lazy-seq 
        (no-nil-cons 
          (issue-code src-row tgt-row)
          (ordered-row-recon (rest src-seq) (rest tgt-seq)))))))

(defn reconcile
  "Executes a reconciliation.  source-params and target-params includes all connection parameters 
  required by clojure.contrib.sql with-connection as well as the following:
  :tblname <name>
  :keycols [seq of names]
  :versioncol <name>
  :touchcol <name> This can be nil for targets
  
  Two different compare types are supported, :version and :timestamp
  
  Recon-func should take two sequences of entities.
  Exception-func should take a sequence of entities."
  [source-params target-params recon-func compare-type exception-func]
  (let [src-seq (entity-seq source-params)
        tgt-seq (entity-seq target-params)]
    (doseq [e (recon-func src-seq tgt-seq)]
      (exception-func e))))
