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
    (log/error "Tables " (:tblname source-params) "/" (:tblname target-params) " out of sync for key " (e 0) " with error code " (e 1))))

(defn- build-where
  "Build the vector of where clause parameters  from the key map as required by clojure.java.jdbc/update-values"
  [keymap]
  (reduce (fn [v kvp]
            (let [s (if (empty? (v 0)) "" " AND ")] ; only add an AND if this isn't the first column
              (apply vector  ; create a new vector of-
                (cons 
                  (str (first v) s (kvp 0) " = ?") ; update the string to have an additional condition
                  (conj (subvec v 1) (kvp 1)))))) ; add the value to be bound to that condition
          [""]
          keymap))

; (reconcile sp tg rf :version (touch-source sp))
(defn touch-source
  "Returns a function that if source params contains the :touchcol option, this handler will update any records 
  that are returned as exceptions except for :src-missing.  Those need to be deleted"
  [source-params]
  (fn [e]
    (sql/with-connection
      (sql/update-values (:tblname source-params)
                         (build-where (e 0))
                         {(:touchcol source-params) (:touchval source-params)}))))
