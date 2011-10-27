(ns jdbcrecon.test.ordered
  (:use [jdbcrecon.core])
  (:use [jdbcrecon.test.testbase])
  (:use [jdbcrecon.ordered])
  (:use [clojure.test]))

(deftest test-ordered-row-recon-no-issues
  (test-no-issues ordered-row-recon))

(deftest test-ordered-row-recon-target-behind
  (test-target-behind ordered-row-recon))

(deftest test-ordered-row-recon-source-behind
  (test-source-behind ordered-row-recon))

(deftest test-missing-record
  (test-missing ordered-row-recon))

(comment
(deftest test-concurrent-missing
  (let [src [[{"key1" 1} 1] [{"key1" 2} 1] [{"key1" 3} 1] [{"key1" 4} 1] [{"key1" 5} 1] [{"key1" 8} 1] [{"key1" 9} 1]]
        tgt [[{"key1" 1} 1] [{"key1" 2} 1] [{"key1" 3} 1] [{"key1" 6} 1] [{"key1" 7} 1] [{"key1" 8} 1] [{"key1" 9} 1]]
        result (ordered-row-recon src tgt)]
    (is (= (count result) 4))
    (is (= (filter #(= (% 1) :src-missing) result) 2))
    (is (= (filter #(= (% 1) :tgt-missing) result) 2))))
)
