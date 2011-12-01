(ns jdbcrecon.test.core
  (:use [jdbcrecon.core])
  (:use [clojure.test]))

(def build-record (ns-resolve 'jdbcrecon.core 'build-record))
(deftest test-build-record
  (let [row {:key1 "Jim" :key2 12345 :ver 1}
        params {:keycols "key1,key2" :versioncol "ver"}
        result (build-record params row)
        keyvec (result 0)]
    (is (= (result 1) 1)) ; Test version number
    (is (= (get keyvec 0) "Jim"))
    (is (= (get keyvec 1) 12345))))

(def build-query (ns-resolve 'jdbcrecon.core 'build-query))
(deftest test-build-query
  (is (= "SELECT key1,key2,key3,version FROM testtable WHERE active = 'Y'"
         (build-query {:tblname "testtable"
                       :keycols "key1,key2,key3"
                       :versioncol "version"
                       :querysuffix "WHERE active = 'Y'"})))
  (is (= "SELECT key1,key2,key3,version FROM testtable"
         (build-query {:tblname "testtable"
                       :keycols "key1,key2,key3"
                       :versioncol "version"}))))
