(ns jdbcrecon.test.inmem
  (:use [jdbcrecon.core])
  (:use [jdbcrecon.inmem])
  (:use [jdbcrecon.test.ordered :only (range-rows)])
  (:use [clojure.test]))

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
