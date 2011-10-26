(ns jdbcrecon.test.inmem
  (:use [jdbcrecon.core])
  (:use [jdbcrecon.inmem])
  (:use [jdbcrecon.test.testbase])
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
  (test-target-behind in-memory-recon)
  (test-source-behind in-memory-recon)
  (test-missing in-memory-recon))
