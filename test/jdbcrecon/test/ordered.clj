(ns jdbcrecon.test.ordered
  (:use [jdbcrecon.core])
  (:use [jdbcrecon.test.testbase])
  (:use [jdbcrecon.ordered])
  (:use [clojure.test]))

(comment
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
)

; test to make sure entity history functions work as designed
(def add-entity (ns-resolve 'jdbcrecon.ordered 'add-entity))
(def new-entity-history (ns-resolve 'jdbcrecon.ordered 'new-entity-history))
(def contains-entity? (ns-resolve 'jdbcrecon.ordered 'contains-entity?))
(def entities-from (ns-resolve 'jdbcrecon.ordered 'entities-from))
(def entities-to (ns-resolve 'jdbcrecon.ordered 'entities-to))
(deftest test-entity-hist-structs
  (let [e (new-entity-history)
        a (add-entity e [{"key1" 1} 1])
        b (reduce #(add-entity %1 %2) e [[{"key1" 1} 1] [{"key1" 2} 2] [{"key1" 3} 3] [{"key1" 4} 4] [{"key1" 5} 5]])
        t (entities-to b [{"key1" 3} 3])
        f (entities-from b [{"key1" 3} 3])]
    (is (set? (e 0)))
    (is (vector? (e 1)))
    (is (set? (a 0)))
    (is (vector? (a 1)))
    (is (contains-entity? a [{"key1" 1} 1]))
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
