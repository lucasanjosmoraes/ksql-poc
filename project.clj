(defproject lucasanjosmoraes/ksql-poc "0.1.0-SNAPSHOT"
  :description "POC of an app integrated with kSQL"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.apache.kafka/kafka-streams "3.2.0"]
                 [io.confluent.ksql/ksqldb-api-client "0.24.0"]
                 [io.confluent.ksql/ksqldb-udf "7.1.1"]
                 [org.clojure/core.async "1.5.648"]]
  :plugins [[lein-ancient "1.0.0-RC3"]]
  :repositories [["ksqlDB" "https://packages.confluent.io/maven/"]]
  :plugin-repositories [["ksqlDB" "https://packages.confluent.io/maven/"]]
  :aliases {"base-resources" ["run" "-m" "lucasanjosmoraes.base-resources"]
            "emulate-app" ["run" "-m" "lucasanjosmoraes.emulate-app"]})
