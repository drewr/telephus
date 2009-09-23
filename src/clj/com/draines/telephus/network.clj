; Cassandra 0.4

(ns com.draines.telephus.network
  (:use [clojure.contrib.def :only (defnk)])
  (:import [org.apache.thrift.protocol TBinaryProtocol TProtocol]
           [org.apache.thrift.transport TSocket TTransport]
           [org.apache.cassandra.service Cassandra$Client ColumnPath ColumnParent]))

(defnk make-transport [:host "localhost" :port 9160]
  (TSocket. host port))

(defn make-proto [tport]
  (TBinaryProtocol. tport))

(comment
  (def tr (make-transport :host "localhost" :port 9160))
  (.open tr)
  (def client (Cassandra$Client. (make-proto tr)))
  (def stime (System/currentTimeMillis))
  (insert client :table "users"
          :id "3"
          :column (ColumnPath. "base_attributes" nil "name")
          :value (.getBytes "Drew Raines" "UTF-8")
          :timestamp stime
          :block-for 0)

  (insert client :table "users"
          :id "3"
          :column (ColumnPath. "base_attributes" nil "age")
          :value (.getBytes "32" "UTF-8")
          :timestamp stime
          :block-for 0)

  (map #(vector
         (.getName %)
         (String. (.getValue %) "UTF-8"))
       (get-since client
                  :table "users"
                  :id "3"
                  :parent (ColumnParent. "base_attributes" nil)
                  :timestamp (- stime 86400000) ;; one day ago
                  ))


  (.close tr)


)

