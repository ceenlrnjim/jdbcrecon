(ns jdbcrecon.test.testbase
  (:use [jdbcrecon.core])
  (:use [clojure.test]))
 
(defn- range-rows
  ([n] (range-rows 0 n 0))
  ([s n] (range-rows s n 0))
  ([s n v] (map #(vector {"key1" %1 "key2" %1} (+ v %1)) (range s n))))

; tests conditions that can be used across implementations
(defn test-no-issues
  [recon-func]
  (let [src-rows (range-rows 10)
        tgt-rows (range-rows 10)]
    (is (= (count (recon-func src-rows tgt-rows)) 0))))

(defn test-target-behind
  [recon-func]
  (let [src-rows (range-rows 0 10 1)
        tgt-rows (range-rows 0 10)
        result (recon-func src-rows tgt-rows)]
    (is (= (count result) 10))
    (is (= (count (filter #(= :version-mismatch (% 1)) result)) 10))))

(defn test-source-behind
  [recon-func]
  (let [src-rows (range-rows 0 10)
        tgt-rows (range-rows 0 10 1)
        result (recon-func src-rows tgt-rows)]
    (is (= (count result) 10))
    (is (= (count (filter #(= :version-mismatch (% 1)) result)) 10))))

(defn test-missing
  [recon-func]
  (let [src-rows (range-rows 0 10)
        tgt-rows (range-rows 0 10)
        missing-row (range-rows 0 9)
        mismatch-row (range-rows 0 10 1)]
    (is (empty? (recon-func src-rows tgt-rows)))
    (is (= [{"key1" 9 "key2" 9} :tgt-missing] (first (recon-func src-rows missing-row))))
    (is (= [{"key1" 9 "key2" 9} :src-missing] (first (recon-func missing-row src-rows))))
    (is (= (count (recon-func src-rows mismatch-row)) 10))))
