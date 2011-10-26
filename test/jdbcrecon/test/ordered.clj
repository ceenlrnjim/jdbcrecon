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

