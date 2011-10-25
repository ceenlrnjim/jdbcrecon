(ns jdbcrecon.test.algo
  (:use [jdbcrecon.core])
  (:use [jdbcrecon.algo])
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

(deftest test-missing-keys
  (let [map1 {{"key1" 1 "key2" 1} 2 {"key1" 2 "key2" 2} 1 {"key1" 3 "key2" 3} 1}
        map2 {{"key1" 1 "key2" 1} 1 {"key1" 2 "key2" 2} 1}
        result (missing-keys map1 map2 :tgt-missing)]
    (is (= (count result) 1))
    (is (= ((first result) 1) :tgt-missing))
    (is (= ((first result) 0) {"key1" 3 "key2" 3}))))

(deftest test-version-mismatch
  (let [map1 {{"key1" 1 "key2" 1} 2 {"key1" 2 "key2" 2} 1 {"key1" 3 "key2" 3} 1}
        map2 {{"key1" 1 "key2" 1} 1 {"key1" 2 "key2" 2} 1}
        result (version-mismatch map1 map2)]
    (is (= (count result) 1))
    (is (= ((first result) 1) :version-mismatch))
    (is (= ((first result) 0) {"key1" 1 "key2" 1}))))

(deftest test-in-memory-recon
  (let [src-rows (range-rows 0 10)
        tgt-rows (range-rows 0 10)
        missing-row (range-rows 0 9)
        mismatch-row (range-rows 0 10 1)]
    (is (empty? (in-memory-recon src-rows tgt-rows)))
    (is (= [{"key1" 9 "key2" 9} :tgt-missing] (first (in-memory-recon src-rows missing-row))))
    (is (= [{"key1" 9 "key2" 9} :src-missing] (first (in-memory-recon missing-row src-rows))))
    (is (= (count (in-memory-recon src-rows mismatch-row)) 10))))
