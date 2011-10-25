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
