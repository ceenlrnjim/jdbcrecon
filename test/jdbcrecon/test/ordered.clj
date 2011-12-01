(ns jdbcrecon.test.ordered
  (:use [jdbcrecon.core])
  (:use [jdbcrecon.test.testbase])
  (:use [jdbcrecon.ordered])
  (:use [clojure.test]))

(deftest test-ordered-row-recon-no-issues
  (test-no-issues ordered-row-recon))

(deftest test-ordered-row-recon-target-behind
  (test-target-behind ordered-row-recon))

(deftest test-ordered-row-recon-source-behind
  (test-source-behind ordered-row-recon))

(deftest test-missing-record
  (test-missing ordered-row-recon))

(deftest test-concurrent-missing-same-size-chunks
  (let [src [[{"key1" 1} 1] [{"key1" 2} 1] [{"key1" 3} 1] [{"key1" 4} 1] [{"key1" 5} 1] [{"key1" 8} 1] [{"key1" 9} 1]]
        tgt [[{"key1" 1} 1] [{"key1" 2} 1] [{"key1" 3} 1] [{"key1" 6} 1] [{"key1" 7} 1] [{"key1" 8} 1] [{"key1" 9} 1]]
        result (ordered-row-recon src tgt)]
    (is (= (count result) 4))
    (is (= (count (filter #(= (% 1) :src-missing) result)) 2))
    (is (= (count (filter #(= (% 1) :tgt-missing) result)) 2))))

(deftest test-concurrent-missing-at-end
  (let [src [[{"key1" 1} 1] [{"key1" 2} 1] [{"key1" 3} 1] [{"key1" 4} 1] [{"key1" 5} 1]]
        tgt [[{"key1" 1} 1] [{"key1" 2} 1] [{"key1" 3} 1] [{"key1" 6} 1] [{"key1" 7} 1]]
        result (ordered-row-recon src tgt)]
    (doseq [r result] (println r))
    (is (= (count result) 4))
    (is (= (count (filter #(= (% 1) :src-missing) result)) 2))
    (is (= (count (filter #(= (% 1) :tgt-missing) result)) 2))
    (is (= (count (filter #(= (% 0) nil) result)) 0))))

; test to make sure record history functions work as designed
(def add-record (ns-resolve 'jdbcrecon.ordered 'add-record))
(def new-record-history (ns-resolve 'jdbcrecon.ordered 'new-record-history))
(def contains-record? (ns-resolve 'jdbcrecon.ordered 'contains-record?))
(def records-from (ns-resolve 'jdbcrecon.ordered 'records-from))
(def records-to (ns-resolve 'jdbcrecon.ordered 'records-to))
(deftest test-record-hist-structs
  (let [e (new-record-history)
        a (add-record e [{"key1" 1} 1])
        b (reduce #(add-record %1 %2) e [[{"key1" 1} 1] [{"key1" 2} 2] [{"key1" 3} 3] [{"key1" 4} 4] [{"key1" 5} 5]])
        t (records-to b [{"key1" 3} 3])
        f (records-from b [{"key1" 3} 3])]
    (is (set? (e 0)))
    (is (vector? (e 1)))
    (is (set? (a 0)))
    (is (vector? (a 1)))
    (is (contains-record? a [{"key1" 1} 1]))
    (is (= (count t) 2))
    (is (= (count f) 3))))

(deftest test-concurrent-missing-smaller-src-chunk
  (let [src [[{"key1" 1} 1] [{"key1" 2} 1] [{"key1" 3} 1] [{"key1" 4} 1] [{"key1" 5} 1] [{"key1" 8} 1] [{"key1" 9} 1]]
        tgt [[{"key1" 1} 1] [{"key1" 2} 1] [{"key1" 6} 1] [{"key1" 7} 1] [{"key1" 8} 1] [{"key1" 9} 1]]
        result (ordered-row-recon src tgt)]
    (doseq [e result] (println e))
    (is (= (count result) 5))
    (is (= (count (filter #(= (% 1) :src-missing) result)) 2))
    (is (= (count (filter #(= (% 1) :tgt-missing) result)) 3))))

(deftest test-resync-with-version-mismatch
  (let [src [[{"abc" 1} 1] [{"abc" 2} 1] [{"abc" 3} 1] [{"abc" 4} 1] [{"abc" 5} 1] [{"abc" 8} 1] [{"abc" 9} 1]]
        tgt [[{"abc" 1} 1] [{"abc" 2} 1] [{"abc" 6} 1] [{"abc" 7} 1] [{"abc" 8} 2] [{"abc" 9} 1]]
        result (ordered-row-recon src tgt)]
    (doseq [e result] (println e))
    (is (= (count result) 6))
    (is (= (count (filter #(= (% 1) :version-mismatch) result)) 1))
    (is (= (count (filter #(= (% 1) :src-missing) result)) 2))
    (is (= (count (filter #(= (% 1) :tgt-missing) result)) 3))))
