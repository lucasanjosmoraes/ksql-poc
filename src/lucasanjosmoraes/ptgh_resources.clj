(ns lucasanjosmoraes.ptgh-resources
  (:require [lucasanjosmoraes.core :as core]
            [lucasanjosmoraes.ptgh-core :as ptgh-core])
  (:import (io.confluent.ksql.api.client Client)))

(defn -main [& _]
  (let [^Client client (core/get-client)]
    (do
      (core/get-server-info client)
      (core/list-tables client)
      (core/list-streams client)
      (core/drop-resource client ptgh-core/DROP_READY_TO_ANALYZE_STREAM_STMT)
      (core/drop-resource client ptgh-core/DROP_PROFILE_DATA_STMT)
      (core/drop-resource client ptgh-core/DROP_ANALYZE_PROFILE_STREAM_STMT)
      (core/drop-resource client ptgh-core/DROP_QUERYABLE_FILES_TABLE_STMT)
      (core/drop-resource client ptgh-core/DROP_FILES_TABLE_STMT)
      (core/drop-resource client ptgh-core/DROP_REGISTER_STREAM_STMT)
      (core/create-table client ptgh-core/CREATE_PROFILE_DATA_TABLE_STMT)
      (core/create-stream client ptgh-core/CREATE_ANALYZE_PROFILE_STREAM_STMT)
      (core/create-stream client ptgh-core/CREATE_READY_TO_ANALYZE_STREAM_STMT)
      (core/create-table client ptgh-core/CREATE_FILES_TABLE_STMT)
      (core/create-table client ptgh-core/CREATE_QUERYABLE_FILES_STMT)
      (core/create-stream client ptgh-core/CREATE_REGISTER_STREAM_STMT)
      (core/async-insert-data client (ptgh-core/get-profile-data-rows))
      (core/async-insert-data client (ptgh-core/get-rows-to-insert))
      (.close client)
      (println "all resources created successfully"))))
