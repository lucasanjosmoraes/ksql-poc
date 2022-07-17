(ns lucasanjosmoraes.emulate-ptgh-app
  (:require [lucasanjosmoraes.core :as core]
            [lucasanjosmoraes.ptgh-core :as ptgh-core])
  (:import (io.confluent.ksql.api.client Client)))

(defn -main [& _]
  (let [^Client client (core/get-client)]
    (do
      (core/get-server-info client)
      (core/list-tables client)
      (core/list-streams client)
      (core/exec-multiple-pull-queries client (ptgh-core/get-queries-to-exec))
      (.close client))))
