(ns lucasanjosmoraes.base-resources
  (:require [lucasanjosmoraes.core :as core])
  (:import (io.confluent.ksql.api.client Client)))

(defn -main [& _]
  (let [^Client client (core/get-client)]
    (do
      (core/get-server-info client)
      (core/list-tables client)
      (core/drop-resource client core/DROP_ANOMALIES_TABLE_STMT)
      (core/drop-resource client core/DROP_TRANSACTIONS_STREAM_STMT)
      (core/create-stream client)
      (core/async-insert-data client)
      (core/create-table client)
      (.close client)
      (println "all resources created successfully"))))