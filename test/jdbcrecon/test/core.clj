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

