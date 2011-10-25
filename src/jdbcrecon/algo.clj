(ns jdbcrecon.algo)

; Contains implementations of the recon function for use with jdbcrecon.core
;
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

