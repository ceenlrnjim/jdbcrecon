(ns jdbcrecon.inmem
  (:require [clojure.tools.logging :as log]))
  

(defn detect-problems
  "filters the keys of map a with the specified filter-function and returns all results as
  exceptions with the specified issue type"
  [a b issue-type filter-func]
  (map
    #(vector % issue-type)
    (filter filter-func (keys a))))

(defn missing-keys
  "Returns a sequence of results for keys that are in a but not in b with the specified issue type.
  a and b are maps of {key-map} to version number"
  [a b issue-type]
  (detect-problems a b issue-type #(not (contains? b %))))

(defn version-mismatch
  "Returns any results that have different version numbers"
  [a b]
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

