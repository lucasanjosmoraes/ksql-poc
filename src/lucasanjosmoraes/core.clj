(ns lucasanjosmoraes.core
  (:import (io.confluent.ksql.api.client ClientOptions Client KsqlObject ExecuteStatementResult ServerInfo Row TableInfo StreamInfo)
           (java.util.concurrent ExecutionException TimeUnit))
  (:require [clojure.core.async :refer [go chan <!! >!!]]))

(def KSQLDB_SERVER_HOST "localhost")
(def KSQLDB_SERVER_PORT 8088)
(def CREATE_TRANSACTIONS_STREAM_STMT "CREATE STREAM transactions (
     tx_id VARCHAR KEY,
     email_address VARCHAR,
     card_number VARCHAR,
     timestamp VARCHAR,
     amount DECIMAL(12, 2)
) WITH (
     kafka_topic = 'transactions',
     partitions = 8,
     value_format = 'avro',
     timestamp = 'timestamp',
     timestamp_format = 'yyyy-MM-dd''T''HH:mm:ss'
);")
(def DROP_TRANSACTIONS_STREAM_STMT "DROP STREAM IF EXISTS TRANSACTIONS DELETE TOPIC;")
(def CREATE_ANOMALIES_TABLE_STMT "CREATE TABLE possible_anomalies WITH (
    kafka_topic = 'possible_anomalies',
    VALUE_AVRO_SCHEMA_FULL_NAME = 'io.ksqldb.tutorial.PossibleAnomaly'
)   AS
    SELECT card_number AS `card_number_key`,
           as_value(card_number) AS `card_number`,
           latest_by_offset(email_address) AS `email_address`,
           count(*) AS `n_attempts`,
           sum(amount) AS `total_amount`,
           collect_list(tx_id) AS `tx_ids`,
           WINDOWSTART as `start_boundary`,
           WINDOWEND as `end_boundary`
    FROM transactions
    WINDOW TUMBLING (SIZE 30 SECONDS, RETENTION 1000 DAYS)
    GROUP BY card_number
    HAVING count(*) >= 3
    EMIT CHANGES;")
(def DROP_ANOMALIES_TABLE_STMT "DROP TABLE IF EXISTS POSSIBLE_ANOMALIES DELETE TOPIC;")
(def SELECT_ANOMALIES_STMT "SELECT * FROM POSSIBLE_ANOMALIES LIMIT 10;")

(def print-ex
  (comp println (partial str "exception handled: ")))

(defn parse-row-to-map
  [^Row row keys-to-select query-tag]
  (let [values     (.getList (.values row))
        columns    (.columnNames row)
        m          (zipmap columns values)
        filtered-m (select-keys m keys-to-select)]
    ;; We can parse filtered-m to an entity with a defined schema.
    (println "Query: " query-tag "Map: " filtered-m)))

(defn wait-n-signals
  [n limit ch]
  (if (not= n limit)
    (do
      (<!! ch)
      (wait-n-signals (inc n) limit ch))
    (println "finished async routine")))

(defn get-client
  []
  (let [options (ClientOptions/create)]
    (doto options
      (.setHost KSQLDB_SERVER_HOST)
      (.setPort KSQLDB_SERVER_PORT))
    (Client/create options)))

(defn get-server-info
  [^Client client]
  (let [^ServerInfo result (-> client
                             (.serverInfo)
                             (.get))]
    (println "kSQL - version:" (.getServerVersion result)
      "cluster-id:" (.getKafkaClusterId result)
      "service-id:" (.getKsqlServiceId result))))

(defn list-streams
  [^Client client]
  (let [result (-> client
                 (.listStreams)
                 (.get))]
    (doall
      (for [^StreamInfo stream result]
        (println "Stream:" (.getName stream)
          "Topic:" (.getTopic stream)
          "Key format:" (.getKeyFormat stream)
          "Value format:" (.getValueFormat stream)
          "Is windowed:" (.isWindowed stream))))))

(defn list-tables
  [^Client client]
  (let [result (-> client
                 (.listTables)
                 (.get))]
    (doall
      (for [^TableInfo table result]
        (println "Table:" (.getName table)
          "Topic:" (.getTopic table)
          "Key format:" (.getKeyFormat table)
          "Value format:" (.getValueFormat table)
          "Is windowed:" (.isWindowed table))))))

(defn create-stream
  [^Client client stmt]
  (try
    (let [^ExecuteStatementResult result (-> client
                                           (.executeStatement stmt
                                             {"auto.offset.reset" "earliest"})
                                           (.get))]
      (println "create-stream - query ID:" (-> result (.queryId) (.orElse "null"))))
    (catch ExecutionException e
      (print-ex (.getMessage (.getCause e))))))

(defn insert-row
  [^String email ^String card ^String tx-id ^String time ^String amount]
  (doto (KsqlObject.)
    (.put "EMAIL_ADDRESS" email)
    (.put "CARD_NUMBER" card)
    (.put "TX_ID" tx-id)
    (.put "TIMESTAMP" time)
    (.put "AMOUNT" (BigDecimal. amount))))

(defn get-rows-to-insert
  []
  {"TRANSACTIONS" (list (insert-row "michael@example.com" "358579699410099" "f88c5ebb-699c-4a7b-b544-45b30681cc39" "2020-04-22T03:19:58" "50.25")
                    (insert-row "derek@example.com" "352642227248344" "0cf100ca-993c-427f-9ea5-e892ef350363" "2020-04-25T12:50:30" "18.97")
                    (insert-row "colin@example.com" "373913272311617" "de9831c0-7cf1-4ebf-881d-0415edec0d6b" "2020-04-19T09:45:15" "12.50")
                    (insert-row "michael@example.com" "358579699410099" "044530c0-b15d-4648-8f05-940acc321eb7" "2020-04-22T03:19:54" "103.43")
                    (insert-row "derek@example.com" "352642227248344" "5d916e65-1af3-4142-9fd3-302dd55c512f" "2020-04-25T12:50:25" "3200.80")
                    (insert-row "derek@example.com" "352642227248344" "d7d47fdb-75e9-46c0-93f6-d42ff1432eea" "2020-04-25T12:51:55" "154.32")
                    (insert-row "michael@example.com" "358579699410099" "c5719d20-8d4a-47d4-8cd3-52ed784c89dc" "2020-04-22T03:19:32" "78.73")
                    (insert-row "colin@example.com" "373913272311617" "2360d53e-3fad-4e9a-b306-b166b7ca4f64" "2020-04-19T09:45:35" "234.65")
                    (insert-row "colin@example.com" "373913272311617" "de9831c0-7cf1-4ebf-881d-0415edec0d6b" "2020-04-19T09:44:03" "150.00"))})

(defn insert-data
  [^Client client result-chan rows-to-insert into]
  (go
    (try
      (doall
        (for [^KsqlObject row-to-insert rows-to-insert]
          (do
            (-> client
              (.insertInto into row-to-insert)
              (.get))
            (println "inserted into: " into)
            (>!! result-chan true))))
      (catch ExecutionException e
        (print-ex (str "insert data - routine: " (.getMessage (.getCause e))))
        (>!! result-chan true)))))

(defn async-insert-data
  [^Client client rows-to-insert]
  (let [qtt-to-insert (reduce #(+ %1 (count %2)) 0 (vals rows-to-insert))
        result-chan   (chan qtt-to-insert)]
    (do
      (doall
        (map (fn [[into rti]] (insert-data client
                                result-chan
                                rti
                                into))
          rows-to-insert))
      (wait-n-signals 0 qtt-to-insert result-chan))))

(defn create-table
  [^Client client stmt]
  (try
    (let [^ExecuteStatementResult result (-> client
                                           (.executeStatement stmt
                                             {"auto.offset.reset" "earliest"})
                                           (.get))]
      (println "create-table - query ID:" (-> result (.queryId) (.orElse "null"))))
    (catch ExecutionException e
      (print-ex (str "create-table - routine: " (.getMessage (.getCause e)))))))

(defn get-queries-to-exec
  []
  {SELECT_ANOMALIES_STMT ["card_number" "email_address" "n_attempts" "tx_ids" "total_amount"]})

(defn exec-pull-query
  [^Client client query keys-to-select]
  (try
    (let [result (-> client
                   (.executeQuery query)
                   (.get))]
      (if (= 0 (count result))
        (println "Empty result")
        (doall
          (for [^Row row result]
            (parse-row-to-map row keys-to-select query)))))
    (catch ExecutionException e
      (print-ex (.getMessage (.getCause e))))))

(defn exec-multiple-pull-queries
  [^Client client queries]
  (doall
    (map (fn [[query keys-to-select]]
           (exec-pull-query client query keys-to-select))
      queries)))

(defn drop-resource
  [^Client client stmt]
  (try
    (let [^ExecuteStatementResult result (-> client
                                           (.executeStatement stmt)
                                           (.get (long 30) (TimeUnit/SECONDS)))]
      (println stmt "- query-id:" (-> result (.queryId) (.orElse "null"))))
    (catch ExecutionException e
      (print-ex (.getMessage (.getCause e))))))
