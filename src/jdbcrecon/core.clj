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

(comment
(defn tgt-behind?
  "Returns true if the src row is ahead of the target row"
  [src-row tgt-row]
  (> (src-row 1) (tgt-row 1)))

(defn src-behind?
  "Returns true if the target row is ahead of the src row"
  [src-row tgt-row]
  (< (src-row 1) (tgt-row 1)))
)

(defn issue-code
  "Returns the issue keyword for the specified rows or nil if there is no issue"
  [src-row tgt-row]
  (if (not= (src-row 1) (tgt-row 1)) :version-mismatch nil))

(defn no-nil-cons
  "Adds item to seq if it is not nil, otherwise returns seq"
  [item seq]
  (if (nil? item) seq (cons item seq)))

(defn ordered-row-recon
  "Reconciliation function that assumes the two sequences share the same ordering.
  Reconciliation is applied as the sequences are consumed.  Useful where the tables are larger
  and we don't want to load all the contents in memory as with other implementations.
  Returns a sequence of [keymap <issue>] where issue is :version-mismatch :tgt-missing :src-missing"
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

(defn detect-problems
  [a b issue-type filter-func]
  (map
    #(vector % issue-type)
    (filter filter-func (keys a))))

(defn missing-keys
  "Returns a sequence of results for keys that are in a but not in b with the specified issue type.
  a and b are maps of {key-map} to version number"
  [a b issue-type]
  (detect-problems a b issue-type #(not (contains? b %))))

; TODO: probably need a defmulti to dispatch on date versus number
; or will default date comparison work - might depend on query
(defn version-mismatch
  "Returns any results that have different version numbers"
  [a b]
  ; TODO: without the contains, this will detect missing keys as well, is that all we need
  (detect-problems a b :version-mismatch #(and (contains? b %) (not= (get a %) (get b %)))))

(defn in-memory-recon
  "Loads all the entities in memory to compare sources.  Order doesn't matter but selection
  time and data size are obviously important"
  [src-seq tgt-seq]
  (let [srcmap (reduce #(assoc %1 (%2 0) (%2 1)) {} src-seq)
        tgtmap (reduce #(assoc %1 (%2 0) (%2 1)) {} tgt-seq)]
    (lazy-cat
      (missing-keys srcmap tgtmap :tgt-missing)
      (missing-keys tgtmap srcmap :src-missing)
      (version-mismatch srcmap tgtmap))))

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
