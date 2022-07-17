(ns lucasanjosmoraes.base-resources
  (:require [lucasanjosmoraes.core :as core])
  (:import (io.confluent.ksql.api.client Client)))

(defn -main [& _]
  (let [^Client client (core/get-client)]
    (do
      (core/get-server-info client)
      (core/list-tables client)
      (core/list-streams client)
      ;; FIXME: search why the schema-registry did a soft delete on the stmt bellow
      (core/drop-resource client core/DROP_ANOMALIES_TABLE_STMT)
      (core/drop-resource client core/DROP_TRANSACTIONS_STREAM_STMT)
      (core/create-stream client core/CREATE_TRANSACTIONS_STREAM_STMT)
      (core/create-table client core/CREATE_ANOMALIES_TABLE_STMT)
      (core/async-insert-data client (core/get-rows-to-insert))
      (.close client)
      (println "all resources created successfully"))))