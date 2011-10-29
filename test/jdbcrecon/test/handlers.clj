(ns jdbcrecon.test.handlers
  (:use [jdbcrecon.handlers])
  (:use [clojure.test]))

(def build-where (ns-resolve 'jdbcrecon.handlers 'build-where))

(deftest test-build-where
  (let [result (build-where {"key1" 1 "key2" "abc"})]
    (is (vector? result))
    (is (= (count result) 3))
    (is (= (result 0) "key1 = ? AND key2 = ?"))
    (is (= (result 1) 1))
    (is (= (result 2) "abc"))))

