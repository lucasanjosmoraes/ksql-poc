(ns lucasanjosmoraes.emulate-app
  (:require [lucasanjosmoraes.core :as core])
  (:import (io.confluent.ksql.api.client Client)))

(defn -main [& _]
  (let [^Client client (core/get-client)]
    (do
      (core/get-server-info client)
      (core/list-tables client)
      (core/exec-pull-query client)
      (.close client))))
