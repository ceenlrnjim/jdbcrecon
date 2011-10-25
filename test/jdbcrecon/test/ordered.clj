(ns jdbcrecon.test.ordered
  (:use [jdbcrecon.core])
  (:use [jdbcrecon.ordered])
  (:use [clojure.test]))

(defn range-rows
  ([n] (range-rows 0 n 0))
  ([s n] (range-rows s n 0))
  ([s n v] (map #(vector {"key1" %1 "key2" %1} (+ v %1)) (range s n))))

(deftest test-ordered-row-recon-no-issues
  (let [src-rows (range-rows 10)
        tgt-rows (range-rows 10)]
    (is (= (count (ordered-row-recon src-rows tgt-rows)) 0))))

(deftest test-ordered-row-recon-target-behind
  (let [src-rows (range-rows 0 10 1)
        tgt-rows (range-rows 0 10)
        result (ordered-row-recon src-rows tgt-rows)]
    (is (= (count result) 10))
    (is (= (count (filter #(= :version-mismatch %1) result)) 10))))

(deftest test-ordered-row-recon-source-behind
  (let [src-rows (range-rows 0 10)
        tgt-rows (range-rows 0 10 1)
        result (ordered-row-recon src-rows tgt-rows)]
    (is (= (count result) 10))
    (is (= (count (filter #(= :version-mismatch %1) result)) 10))))

