(ns jdbcrecon.ordered
  (:use [jdbcrecon.core])
  (:require [clojure.tools.logging :as log]))

; Contains implementations of the recon function for use with jdbcrecon.core
;
(defn- versions-match?
  "Returns true if the versions on the specified records match"
  [src-row tgt-row]
  (= (record-version src-row) (record-version tgt-row)))

(defn- keysmatch?
  "returns true if the keys of the two records are the same"
  [a b]
  (= (record-key a) (record-key b)))

(defn- except-remainder
  "Returns all the records in the sequence as issues with the specified code"
  [s issue]
  (map #(vector (record-key %) issue) s))

(declare ordered-row-recon)

; TODO: src-hist and tgt-hist: would like to have maps for fast lookup (contains?)
; but need an ordered sequence for re-winding to find sync points.
; Basically want a map that maintains the add order.
; I could keep both a vector and a map, but then I use more memory when syncing blocks
; of concurrently missing data
;
; Moving to these functions to be able to change implementations 
; Currently using a set of keys and a vector of records for replay
(defn- new-record-history
  "Returns a data structure for holding items that have been read from the stream but not processed"
  []
  (vector #{} []))

; need to just compare the key, not the whole object in case the re-sync point has different version numbers
(defn- contains-record?
  "Returns true if this record is in the history"
  [hist e]
  (contains? (hist 0) (record-key e)))

(defn- add-record
  "Adds an record to the history"
  [hist e]
  (vector (conj (hist 0) (record-key e)) (conj (hist 1) e))) ; conj at end for vector

(defn- records-to
  "Returns the records in the history up to (but not including) the one specified"
  [hist e]
  (take-while #(not (keysmatch? % e)) (hist 1)))

(defn- records-from
  "Returns the records in the history from the one specified to the end"
  [hist e]
  (drop-while #(not (keysmatch? % e)) (hist 1)))

(defn- all-records
  "returns ordered list of all records in the history"
  [hist]
  (hist 1))
; ------------------- End history functions designed to hide data structure to allow changes
(defn- sync-at-both-seqs-empty
  "Handles case where we've come to the end of both streams without finding a a place to resynchronize.
  Everything in both histories are missing from the other source."
  [src-hist tgt-hist]
  (log/debug "Both sequences empty, emitting remainders of both")
  (lazy-cat (except-remainder (all-records src-hist) :tgt-missing)
            (except-remainder (all-records tgt-hist) :src-missing)))

(defn- sync-at-empty-seq
  "Handles the case where one of the two sequences run out of items before finding a sync point."
  [non-empty-seq issue tgt-missing-hist src-missing-hist]
  (log/debug "Sequence empty")
  (lazy-cat (except-remainder (all-records tgt-missing-hist) :tgt-missing)
            (except-remainder (all-records src-missing-hist) :src-missing)
            (except-remainder non-empty-seq issue)))

(defn- sync-at-matching-items
  "Handles the case where the next two items in the sequences are the sync point"
  [src-seq tgt-seq src-hist tgt-hist]
  (lazy-cat (except-remainder src-hist :tgt-missing)
            (except-remainder tgt-hist :src-missing)
            (ordered-row-recon src-seq tgt-seq)))

(defn- sync-at-src-hist
  "Handles the case where the next item in the target sequence needs to be synchronized with a point in
  the source history"
  [src-seq s tgt-seq t src-hist tgt-hist]
  (log/debug "Found target in source history")
  (lazy-cat 
    ; Need to emit all of tgt-hist as :src-missing
    (except-remainder (all-records tgt-hist) :src-missing)
    ; Need to get pre-t entries from src-hist and emit as :tgt-missing
    (except-remainder (records-to src-hist t) :tgt-missing)
    ; drop-while will have t in it, so we don't want to (rest) the sequence
    ; rewind the source seq to point t and add to the rest of src-seq
    (ordered-row-recon (lazy-cat (records-from src-hist t) src-seq) tgt-seq)))

(defn- sync-at-tgt-hist
  "Handles the case where the next item in the source sequence needs to be synchronized with a point in
  the target history"
  [src-seq s tgt-seq t src-hist tgt-hist]
  (log/debug "Found source " s " in target history")
  (lazy-cat 
    ; Need to emit all of src-hist as :tgt-missing
    (except-remainder (all-records src-hist) :tgt-missing)
    ; Need to get pre-s entries from tgt-hist and emit as :src-missing
    (except-remainder (records-to tgt-hist s) :src-missing)
    (ordered-row-recon src-seq (lazy-cat (records-from tgt-hist s) tgt-seq))))

(defn- resync-seqs-iter
  "Iterative implementation of the function that re-synchronizes the streams to find the next common point and emits
  exceptions for everything that is missing from either sequence.  As we move forward through the sequences we keep
  any un-reconciled records in the history structures.  Once we have found the sync point, we'll need to report on these
  temporarily skipped items.
  
  This function detects the sync point and calls the appropriate function to resync the streams"
  [src-seq tgt-seq src-hist tgt-hist]
  (let [s (first src-seq)
        t (first tgt-seq)]
    (log/debug "Comparing " s " with " t " with histories " src-hist " and " tgt-hist)
    (cond 
      ; Both sequences are empty
      (and (empty? src-seq) (empty? tgt-seq)) (sync-at-both-seqs-empty src-hist tgt-hist)
      ; Source sequence is empty
      (empty? src-seq) (sync-at-empty-seq tgt-seq :src-missing src-hist tgt-hist)
      ; Target sequence is empty
      (empty? tgt-seq) (sync-at-empty-seq src-seq :tgt-missing src-hist tgt-hist)
      ; next items match
      (= s t) (sync-at-matching-items src-seq tgt-seq src-hist tgt-hist)
      ; target item is in source history
      (contains-record? src-hist t) (sync-at-src-hist src-seq s tgt-seq t src-hist tgt-hist)
      ; source item is in target history
      (contains-record? tgt-hist s) (sync-at-tgt-hist src-seq s tgt-seq t src-hist tgt-hist)
      ; No match yet, keep going
      :else (recur (rest src-seq) 
                   (rest tgt-seq)
                   (add-record src-hist s)
                   (add-record tgt-hist t)))))

(defn resync-seqs
  "Advances both streams to the next record that is in both.  All intermediary entites are returned with
  either :tgt-missing or :src-missing and the remained are passed to ordered-row-recon"
  [src-seq tgt-seq]
  (resync-seqs-iter src-seq tgt-seq (new-record-history) (new-record-history)))

(defn ordered-row-recon
  "Reconciliation function that assumes the two sequences share the same ordering.
  Reconciliation is applied as the sequences are consumed.  Useful where the tables are larger
  and we don't want to load all the contents in memory as with other implementations.
  Returns a sequence of [keymap <issue>] where issue is :version-mismatch :tgt-missing :src-missing.
  Large gaps of missing data in both streams at the same point will generate overhead in this implementation"
  [src-seq tgt-seq]
  (let [src-row (first src-seq)
        tgt-row (first tgt-seq)]
    (cond 
          ; if both sequences are empty, terminate
          (and (empty? tgt-seq) (empty? src-seq)) nil
          ; if the source is empty but the target has more items, all remaining items are missing from the source
          (empty? src-seq) (except-remainder tgt-seq :src-missing)
          ; if the target is empty but the source has more items, all remaining items are missing from the target
          (empty? tgt-seq) (except-remainder src-seq :tgt-missing)
          ; if the keys of the next items don't match, the sequences are out of sync (due to missing items)
          ; we need to fast forward the streams to the next point at which the keys match again.
          ; All the complexity of this implementation lies here
          (not (keysmatch? src-row tgt-row)) (resync-seqs src-seq tgt-seq)
          ; if the versions are different, report the exception and continue
          (not (versions-match? src-row tgt-row)) (lazy-seq (cons 
                                                              (vector (src-row 0) :version-mismatch)
                                                              (ordered-row-recon (rest src-seq) (rest tgt-seq))))
          ; otherwise all is well with this record, continue to the next
          :else (recur (rest src-seq) (rest tgt-seq)))))
