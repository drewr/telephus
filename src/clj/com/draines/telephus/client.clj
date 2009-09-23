; Cassandra 0.4

(ns com.draines.telephus.client
  (:use [clojure.contrib.def :only (defnk)]
        [com.draines.telephus.network :only (make-transport make-proto)])
  (:import [org.apache.cassandra.service Cassandra$Client]
           [org.apache.cassandra.service Column ColumnPath ColumnParent]
           [org.apache.cassandra.service BatchMutation]
           [org.apache.cassandra.service InvalidRequestException]))

(defn make-client [& opts]
  (let [tr (apply make-transport opts)
        proto (make-proto tr)]
    (-> tr .open)
    {:transport tr
     :protocol proto
     :cli (Cassandra$Client. proto)}))

(defn close [client]
  (-> (:transport client) .close))

(defmacro with-client [[sym] & body]
  `(let [~sym (make-client)
         res# ~@body]
     (close ~sym)
     res#))

(defnk make-batch [:rowkey nil :family nil :pairs nil :timestamp (System/currentTimeMillis)]
  (let [columns (map #(Column. (first %) (second %) timestamp) pairs)]
    (when rowkey
      (BatchMutation. rowkey {family columns}))))

(defnk insert [client :table nil :rowkey nil :column nil
               :value nil :timestamp (System/currentTimeMillis)
               :block-for 0]
  (-> (:cli client) (.insert table rowkey column value timestamp block-for)))

(defnk insert-batch [client :table nil :batch nil :block-for 0]
  (-> (:cli client) (.batch_insert table batch block-for)))

(defnk get-since [client :table nil :rowkey nil :parent nil
                  :timestamp (System/currentTimeMillis)]
  (-> (:cli client) (.get_columns_since table rowkey parent timestamp)))

(defnk get-slice-by-names [client :table nil :rowkey nil
                           :parent "" :names []]
  (-> (:cli client) (.get_slice_by_names table rowkey parent names)))

(defnk get-key-range [client :table nil :family nil
                      :start-with "" :stop-at "" :max 100]
  (-> (:cli client) (.get_key_range table family start-with stop-at max)))

(defnk get-slice [client :table nil :rowkey nil :parent nil
                  :start "" :finish "" :ascending? false :count 100]
  (-> (:cli client) (.get_slice table rowkey parent start finish ascending? count)))

(defnk get-column [client :table nil :rowkey nil :path nil]
  (-> (:cli client) (.get_column table rowkey path)))

(comment
  (def client (make-client))

  (insert client :table "users"
          :rowkey "3"
          :column (ColumnPath. "base_attributes" nil "name")
          :value (.getBytes "Foo Bar" "UTF-8")
          :block-for 0)

  (insert client :table "users"
          :rowkey "3"
          :column (ColumnPath. "base_attributes" nil "age")
          :value (.getBytes "21" "UTF-8")
          :block-for 0)


  (try
   (insert client :table "WebMessenger"
           :rowkey "20090722:FOO"
           :column (ColumnPath. "messages" nil "foo")
           :value (.getBytes "21" "UTF-8")
           :block-for 0)
   (catch InvalidRequestException e
     (.why e)))

  (get-slice-by-names client
                      :table "users"
                      :rowkey "3"
                      :parent (ColumnParent. "base_attributes" nil)
                      :names ["name" "age"])

  (try
   (map class (get-key-range client
                             :table "users"
                             :family "base_attributes"
                             :start-with "1"
                             :stop-at "5"))
   (catch InvalidRequestException e
     (.why e)))

  (try
   (get-key-range client
                  :table "WebMessenger"
                  :family "messages"
                  :start-with "20090501:6515001"
                  :stop-at "20090501:6515005"
                  :max 5)
   (catch InvalidRequestException e
     (.why e)))

  (try
   (get-slice client
              :table "WebMessenger"
              :rowkey "20090501:6515006_2880016554_1507557"
              :parent (ColumnParent. "messages" nil))
   (catch InvalidRequestException e
     (.why e)))

  (try
   (get-column client
              :table "users"
              :rowkey "3"
              :parent (ColumnPath. "base_attributes" nil "name"))
   (catch InvalidRequestException e
     (.why e)))

  (-> (:cli client)
      (.batch_insert
       "users"
       (BatchMutation. "4"
                       {"base_attributes" [(Column. "name"
                                                    (.getBytes "Rick Astley")
                                                    (System/currentTimeMillis))
                                           (Column. "age"
                                                    (.getBytes "43")
                                                    (System/currentTimeMillis))]})
       0))

  (map #(vector
         (.getName %)
         (String. (.getValue %) "UTF-8"))
       (get-since client
                  :table "users"
                  :rowkey "4"
                  :parent (ColumnParent. "base_attributes" nil)
                  :timestamp (- (System/currentTimeMillis) 86400000) ;; one day ago
                  ))

  (close client)

  (count
   (with-client [client]
     (get-slice client
                :table "WebMessenger"
                :rowkey "20090501:6515099_2880016593_1507965"
                :parent (ColumnParent. "messages" nil)
                :ascending? true)))

)

