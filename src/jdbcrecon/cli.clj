(ns jdbcrecon.cli
  (:use [jdbcrecon.core])
  (:use [jdbcrecon.inmem])
  (:use [jdbcrecon.ordered])
  (:use [jdbcrecon.handlers])
  (:require [clojure.tools.cli :as cli])
  (:require [clojure.tools.logging :as log]))

(defn- load-params
  "Loads a property file into a map and converts keys to keywords"
  [filename]
  (let [props (java.util.Properties.)]
    (with-open [s (java.io.FileInputStream. filename)]
      (.load props s))
    (reduce #(assoc %1 (keyword (.getKey %2)) (.getValue %2)) {} props)))

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


(defn -main
  "entry point for command line version of the recon"
  [& args]
  (let [args-map (cli/cli args
                      (cli/required ["-s" "--source" "The name of the file with the source parameters"])
                      (cli/required ["-t" "--target" "The name of the file with the target parameters"])
                      (cli/required ["-r" "--recon-algo" "The name of the recon algorithm - inmem or ordered" :default "inmem"])
                      (cli/required ["-h" "--handler" "the name of the handler to be used" :default "log"]))
        unused (log/debug args-map)
        source-params (load-params (:source args-map))
        target-params (load-params (:target args-map))]
    (log/debug "Source params: " source-params)
    (log/debug "Target params: " target-params)
    (reconcile source-params 
               target-params 
               (recon-func (:recon-algo args-map))
               nil ; currently unused
               (handler-func (:handler args-map) source-params target-params))))
