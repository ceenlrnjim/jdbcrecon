(ns jdbcrecon.ordered)

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
