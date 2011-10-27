(ns jdbcrecon.ordered)

; Contains implementations of the recon function for use with jdbcrecon.core
;
(defn versions-match?
  "Returns the issue keyword for the specified rows or nil if there is no issue"
  [src-row tgt-row]
  (= (src-row 1) (tgt-row 1)))

(defn no-nil-cons
  "Adds item to seq if it is not nil, otherwise returns seq"
  [item seq]
  (if (nil? item) seq (cons item seq)))

(defn- keysmatch?
  "returns true if two entity sequence members have the same key"
  [a b]
  (= (a 0) (b 0)))

(defn- except-remainder
  "Returns all the entities in the sequence as issues with the specified code"
  [s issue]
  (map #(vector (% 0) issue) s))

(declare ordered-row-recon)

; TODO: src-hist and tgt-hist: would like to have maps for fast lookup (contains?)
; but need an ordered sequence for re-winding to find sync points.
; Basically want a map that maintains the add order.
; I could keep both a vector and a map, but then I use more memory when syncing blocks
; of concurrently missing data
;
; Moving to these functions to be able to change implementations 
(defn- new-entity-history
  "Returns a data structure for holding items that have been read from the stream but not processed"
  []
  (vector #{} []))

(defn- contains-entity?
  "Returns true if this entity is in the history"
  [hist e]
  (contains? (hist 0) e))

(defn- add-entity
  "Adds an entity to the history"
  [hist e]
  (vector (conj (hist 1) e) (conj (hist 0) e))) ; conj at end for vector

(defn- entities-to
  "Returns the entities in the history up to (but not including) the one specified"
  [hist e]
  (take-while #(not= % e) (hist 1)))

(defn- entities-from
  "Returns the entities in the history from the one specified to the end"
  [hist e]
  (drop-while #(not= % e) (hist 1)))

(defn- all-entities
  "returns ordered list of all entities in the history"
  [hist]
  (hist 1))
; ------------------- End history functions designed to hide data structure to allow changes

(defn- resync-seqs-iter
  "If you have large blocks of missing data this implementation may kill you"
  [src-seq tgt-seq src-hist tgt-hist]
  (let [s (first src-seq)
        t (first tgt-seq)]
    (println "Comparing " s " with " t " with histories " src-hist " and " tgt-hist)
    (cond (= s t)
            (lazy-cat (except-remainder src-hist :tgt-missing)
                      (except-remainder tgt-hist :src-missing)
                      (ordered-row-recon src-seq tgt-seq))
          (contains-entity? src-hist t) 
                      ; Need to emit all of tgt-hist as :src-missing
            (lazy-cat (except-remainder (all-entities tgt-hist) :src-missing
                      ; Need to get pre-t entries from src-hist and emit as :tgt-missing
                      (except-remainder (entities-to src-hist t) :tgt-missing)
                      ; drop-while will have t in it, so we don't want to (rest) the sequence
                      ; rewind the source seq to point t and add to the rest of src-seq
                     (ordered-row-recon (lazy-cat (entities-from src-hist t) src-seq) tgt-seq)))
          (contains-entity? tgt-hist s) 
                      ; Need to emit all of src-hist as :tgt-missing
            (lazy-cat (except-remainder (all-entities src-hist) :tgt-missing)
                      ; Need to get pre-s entries from tgt-hist and emit as :src-missing
                      (except-remainder (entities-to tgt-hist s) :src-missing)
                      (ordered-row-recon src-seq (lazy-cat (entities-from tgt-hist s) tgt-seq)))
          :else (recur (rest src-seq) 
                       (rest tgt-seq)
                       (add-entity src-hist s)
                       (add-entity tgt-hist t)))))

(defn resync-seqs
  "Advances both streams to the next entity that is in both.  All intermediary entites are returned with
  either :tgt-missing or :src-missing and the remained are passed to ordered-row-recon"
  [src-seq tgt-seq]
  (resync-seqs-iter src-seq tgt-seq (new-entity-history) (new-entity-history)))

(defn ordered-row-recon
  "Reconciliation function that assumes the two sequences share the same ordering.
  Reconciliation is applied as the sequences are consumed.  Useful where the tables are larger
  and we don't want to load all the contents in memory as with other implementations.
  Returns a sequence of [keymap <issue>] where issue is :version-mismatch :tgt-missing :src-missing.
  Large gaps of missing data in both streams at the same point will generate overhead in this implementation"
  [src-seq tgt-seq]
  (let [src-row (first src-seq)
        tgt-row (first tgt-seq)]
    (cond (and (empty? tgt-seq) (empty? src-seq)) nil
          (empty? src-seq) (except-remainder tgt-seq :src-missing)
          (empty? tgt-seq) (except-remainder src-seq :tgt-missing)
          (not (keysmatch? src-row tgt-row)) (resync-seqs src-seq tgt-seq)
          (not (versions-match? src-row tgt-row)) (lazy-seq (cons 
                                                              (vector (src-row 0) :version-mismatch)
                                                              (ordered-row-recon (rest src-seq) (rest tgt-seq))))
          :else (ordered-row-recon (rest src-seq) (rest tgt-seq)))))
