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

(defn- resync-seqs-iter
  "If you have large blocks of missing data this implementation may kill you"
  [src-seq tgt-seq src-hist tgt-hist]
  (let [s (first src-seq)
        t (first tgt-seq)]
    (cond (contains? src-hist t) 
                      ; Need to emit all of tgt-hist as :src-missing
            (lazy-cat (except-remainder tgt-hist :src-missing
                      ; Need to get pre-t entries from src-hist and emit as :tgt-missing
                      (except-remainder (take-while #(not= % t) src-hist) :tgt-missing)
                      ; drop-while will have t in it, so we don't want to (rest) the sequence
                      ; rewind the source seq to point t and add to the rest of src-seq
                     (ordered-row-recon (lazy-cat (drop-while #(not= % t) src-hist) src-seq) tgt-seq)))
          (contains? tgt-hist s) 
                      ; Need to emit all of src-hist as :tgt-missing
            (lazy-cat (except-remainder src-hist :tgt-missing)
                      ; Need to get pre-s entries from tgt-hist and emit as :src-missing
                      (except-remainder (take-while #(not= % s) tgt-hist) :src-missing)
                      (ordered-row-recon src-seq (lazy-cat (drop-while #(not= % s) tgt-hist) tgt-seq)))
          :else (recur (rest src-seq) 
                       (rest tgt-seq)
                       (conj src-hist s)
                       (conj tgt-hist s)))))

(defn resync-seqs
  "Advances both streams to the next entity that is in both.  All intermediary entites are returned with
  either :tgt-missing or :src-missing and the remained are passed to ordered-row-recon"
  [src-seq tgt-seq]
  (resync-seqs-iter src-seq tgt-seq [] []))

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
