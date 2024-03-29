(ns jdbcrecon.batch
  (:use [jdbcrecon.core])
  (:use [jdbcrecon.inmem])
  (:use [jdbcrecon.ordered])
  (:use [jdbcrecon.handlers])
  (:require [clojure.xml :as xml])
  (:require [clojure.tools.cli :as cli])
  (:require [clojure.tools.logging :as log]))

; XML format
; <config>
;   <source-db-params>
;     <param name="" value=""/>
;   </source-db-params>
;   <target-db-params>
;     <param name="" value=""/>
;   </target-db-params>
; <tables>
;   <table algo="inmem" handler="log">
;     <source tblname='xyz' versioncol='123' keycols="123,456"/>
;     <target tblname="sxsd" versioncol="123" keycols="123,678"/>
;   </table>
; </tables>
; </config>
;
;

(defn- child-tags
  "Returns a sequence of child tags in the xml tree with the specified name that are
  children of the specified node."
  [tree child]
  (filter #(= (xml/tag %) (keyword child)) (xml/content tree)))


(defn- build-db-params
  [config param-tag]
  ; get to the collection tag level (source-params or target-params
  (let [cfgroot (first (child-tags config param-tag))]
    ; Iterate through each entry (should be a param tag) and create a keyword/value pair for the name/value attributes
    (reduce #(assoc %1 (keyword (:name (xml/attrs %2))) (:value (xml/attrs %2))) {} (xml/content cfgroot))))

(defn- recon-func
  "Returns the appropriate recon function based on the recon-algo argument"
  [name]
  (cond (= name "ordered") ordered-row-recon
        :else in-memory-recon))

(defn- handler-func
  "Returns the appropriate handler function based on the handler argument"
  [name sp tp]
  (cond (= name "touch") (touch-source sp)
        :else (exception-logger sp tp)))


(defn- build-recons
  "Creates a sequence of no-argument thunks that will execute the recons specified in the configuration"
  [config sp tp]
  (map #(fn [] (reconcile (merge (xml/attrs (first (child-tags % "source"))) sp) ; put all the keys from the xml element into the map
                          (merge (xml/attrs (first (child-tags % "target"))) tp)
                          (recon-func (:algo (xml/attrs %)))
                          nil ; unused argument compare-type
                          (handler-func (:handler (xml/attrs %)) sp tp)))
    (child-tags (first (child-tags config "tables")) "table")))

(defn batch-recon
  "Parses the specified XML config file and returns a sequence of zero argument functions, one for each recon job.
  Config Name can be anything that that clojure.xml (parse) supports"
  [configName]
  (let [config (xml/parse configName)
        source-params (build-db-params config "source-db-params")
        target-params (build-db-params config "target-db-params")]
    (build-recons config source-params target-params)))

(defn exec
  [recon-seq]
  (doseq [r recon-seq] (r)))

(defn pexec
  [recon-seq]
  (pmap #(%) recon-seq))

(comment
  ; removing scheduling functionality - use cron or other scheduling software to run a batch of recons
  ; don't want to duplicate that functionality here
(defn schedule
  "Executest the specified config file with the specified exec function according to the specified schedule.
  Currently schedule just supports number of minutes to sleep between executions"
  [configName execFunc schedule]
  (let [funcs (batch-recon configName)]
    (while true
      (execFunc funcs)
      (Thread/sleep (* 1000 60 schedule)))))
)
