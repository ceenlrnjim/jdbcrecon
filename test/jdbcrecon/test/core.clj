(ns jdbcrecon.test.core
  (:use [jdbcrecon.core])
  (:use [clojure.test]))

(deftest test-build-entity
  (let [row {:key1 "Jim" :key2 12345 :ver 1}
        params {:keycols ["key1" "key2"] :versioncol "ver"}
        result (build-entity params row)
        keymap (result 0)]
    (is (= (result 1) 1)) ; Test version number
    (is (= (get keymap "key1") "Jim"))
    (is (= (get keymap "key2") 12345))))

(deftest test-build-query
  (is (= "SELECT key1,key2,key3,version FROM testtable WHERE active = 'Y'"
         (build-query {:tblname "testtable"
                       :keycols ["key1" "key2" "key3"]
                       :versioncol "version"
                       :querysuffix "WHERE active = 'Y'"})))
  (is (= "SELECT key1,key2,key3,version FROM testtable"
         (build-query {:tblname "testtable"
                       :keycols ["key1" "key2" "key3"]
                       :versioncol "version"}))))

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
