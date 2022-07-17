(ns lucasanjosmoraes.emulate-app
  (:require [lucasanjosmoraes.core :as core])
  (:import (io.confluent.ksql.api.client Client)))

(defn -main [& _]
  (let [^Client client (core/get-client)]
    (do
      (core/get-server-info client)
      (core/list-tables client)
      (core/list-streams client)
      (core/exec-multiple-pull-queries client (core/get-queries-to-exec))
      (.close client))))
