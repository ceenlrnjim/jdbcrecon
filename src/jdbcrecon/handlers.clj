(ns jdbcrecon.handlers
  (:require [clojure.java.jdbc :as sql])
  (:require [clojure.tools.logging :as log]))

; Collection of simple handlers for recon exceptions
;

; (reconcile sp tg rf :version (exception-logger sp tp))
(defn exception-logger
  "Returns a function suitable for processing recon exceptions that logs to log4j"
  [source-params target-params]
  (fn [e]
    (log/error (str "Tables " (:tblname source-params) "/" (:tblname target-params) " out of sync for key " (e 0) " with error code " (e 1)))))

(defn- build-where
  [keymap]
  nil) ; TODO

(defn touch-source
  "Returns a function that if source params contains the :touchcol option, this handler will update any records 
  that are returned as exceptions except for :src-missing.  Those need to be deleted"
  [source-params]
  (fn [e]
    (sql/with-connection
      (sql/update-values (:tblname source-params)
                         (build-where (e 0))
                         (:touchval source-params)))))
