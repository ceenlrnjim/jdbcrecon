(ns jdbcrecon.test.batch
  (:use [jdbcrecon.core])
  (:use [jdbcrecon.batch])
  (:use [jdbcrecon.test.testbase])
  (:require [clojure.xml :as xml])
  (:use [clojure.test]))

(def test-config
" <config>
   <source-db-params>
     <param name='classname' value='com.mysql.jdbc.Driver'/>
     <param name='subprotocol' value='mysql'/>
     <param name='subname' value='//localhost:3306/jdbcrecon'/>
     <param name='user' value='jdbcrecon_user'/>
     <param name='password' value='mysql'/>
   </source-db-params>
   <target-db-params>
     <param name='classname' value='com.mysql.jdbc.Driver'/>
     <param name='subprotocol' value='mysql'/>
     <param name='subname' value='//localhost:3306/jdbcrecon'/>
     <param name='user' value='jdbcrecon_user'/>
     <param name='password' value='mysql'/>
   </target-db-params>
 <tables>
   <table algo='inmem' handler='log'>
     <source tblname='testone' versioncol='version' keycols='test_id,name'/>
     <target tblname='testtwo' versioncol='ver' keycols='two_id,two_nm'/>
   </table>
   <!-- Wouldn't normally do this, just want to have multiple tables in the test config -->
   <table algo='ordered' handler='log'>
     <source tblname='testone' versioncol='version' keycols='test_id,name'/>
     <target tblname='testtwo' versioncol='ver' keycols='two_id,two_nm'/>
   </table>
 </tables>
 </config>
")

(def child-tags (ns-resolve 'jdbcrecon.batch 'child-tags))
(deftest test-child-tags
  (let [config (xml/parse (java.io.ByteArrayInputStream. (.getBytes test-config)))
        source-tags (child-tags config "source-db-params")]
    (is (= (count source-tags) 1))
    (is (= (xml/tag (first source-tags)) :source-db-params))
    (let [param-tags (child-tags (first source-tags) "param")]
      (is (= (count param-tags) 5)))))

(def build-db-params (ns-resolve 'jdbcrecon.batch 'build-db-params))
(deftest test-build-db-params
  (let [config (xml/parse (java.io.ByteArrayInputStream. (.getBytes test-config)))
        sp (build-db-params config "source-db-params")]
    (is (= (:classname sp) "com.mysql.jdbc.Driver"))
    (is (= (:subname sp) "//localhost:3306/jdbcrecon"))
    (is (= (:subprotocol sp) "mysql"))
    (is (= (:user sp) "jdbcrecon_user"))
    (is (= (:password sp) "mysql"))))

(def build-recons (ns-resolve 'jdbcrecon.batch 'build-recons))
(deftest test-build-recons
  (let [config (xml/parse (java.io.ByteArrayInputStream. (.getBytes test-config)))
        sp (build-db-params config "source-db-params")
        tp (build-db-params config "target-db-params")
        result (build-recons config sp tp)]
    (is (= (count result) 2))
    (is (= (count (filter #(fn? %) result)) 2))))

