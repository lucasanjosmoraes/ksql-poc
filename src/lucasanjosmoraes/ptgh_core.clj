(ns lucasanjosmoraes.ptgh-core
  (:import (io.confluent.ksql.api.client KsqlObject)
           (java.util Date)
           (java.text SimpleDateFormat)))

;; ksql will partition this table according to the JOIN requirements
(def CREATE_PROFILE_DATA_TABLE_STMT "CREATE TABLE profile_data (
    id VARCHAR PRIMARY KEY,
    email VARCHAR,
    username VARCHAR,
    password VARCHAR,
    created_at VARCHAR
) WITH (
    kafka_topic = 'profile_data',
    partitions = 1,
    value_format = 'avro',
    timestamp = 'created_at',
    timestamp_format = 'yyyy-MM-dd''T''HH:mm:ss',
    VALUE_AVRO_SCHEMA_FULL_NAME = 'com.lucasanjosmoraes.ProfileData'
);")
(def DROP_PROFILE_DATA_STMT "DROP TABLE IF EXISTS PROFILE_DATA DELETE TOPIC;")

;; ksql will partition this stream according to the JOIN requirements
(def CREATE_ANALYZE_PROFILE_STREAM_STMT "CREATE STREAM analyze_profile (
    id VARCHAR,
    profile_id VARCHAR,
    state VARCHAR
) WITH (
    kafka_topic = 'analyze_profile',
    partitions = 1,
    value_format = 'avro'
);")
(def DROP_ANALYZE_PROFILE_STREAM_STMT "DROP STREAM IF EXISTS ANALYZE_PROFILE DELETE TOPIC;")

;; https://docs.ksqldb.io/en/latest/developer-guide/joins/partition-data/#co-partitioning-requirements
(def CREATE_READY_TO_ANALYZE_STREAM_STMT "CREATE STREAM ready_to_analyze AS
    SELECT
      ap.state,
      pd.id,
      pd.email,
      pd.username,
      pd.password,
      pd.created_at
    FROM analyze_profile ap
      JOIN profile_data pd ON ap.profile_id = pd.id
    EMIT CHANGES;")
(def DROP_READY_TO_ANALYZE_STREAM_STMT "DROP STREAM IF EXISTS READY_TO_ANALYZE DELETE TOPIC;")
(def SELECT_READY_TO_ANALYZE_STMT "SELECT * FROM READY_TO_ANALYZE LIMIT 10;")

(def CREATE_FILES_TABLE_STMT "CREATE TABLE files (
    id VARCHAR PRIMARY KEY,
    profile_id VARCHAR,
    url VARCHAR,
    type VARCHAR
) WITH (
    kafka_topic = 'files',
    partitions = 2,
    value_format = 'avro',
    VALUE_AVRO_SCHEMA_FULL_NAME = 'com.lucasanjosmoraes.Files'
);")
(def CREATE_QUERYABLE_FILES_STMT "CREATE TABLE QUERYABLE_FILES AS SELECT * FROM FILES EMIT CHANGES;")
(def DROP_QUERYABLE_FILES_TABLE_STMT "DROP TABLE IF EXISTS QUERYABLE_FILES DELETE TOPIC;")
(def DROP_FILES_TABLE_STMT "DROP TABLE IF EXISTS FILES DELETE TOPIC;")
(def SELECT_FILES_STMT "SELECT * FROM QUERYABLE_FILES LIMIT 10;")

(def CREATE_REGISTER_STREAM_STMT "CREATE STREAM register (
    id VARCHAR,
    email VARCHAR,
    username VARCHAR,
    password VARCHAR,
    profile_id VARCHAR,
    created_at VARCHAR
) WITH (
    kafka_topic = 'register',
    partitions = 2,
    value_format = 'avro',
    timestamp = 'created_at',
    timestamp_format = 'yyyy-MM-dd''T''HH:mm:ss'
);")
(def DROP_REGISTER_STREAM_STMT "DROP STREAM IF EXISTS REGISTER DELETE TOPIC;")
(def SELECT_REGISTER_STMT "SELECT * FROM REGISTER LIMIT 10;")

(defn now
  []
  (.format (SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss") (Date.)))

(defn insert-profile-data
  [^String id ^String email ^String username ^String password]
  (doto (KsqlObject.)
    (.put "ID" id)
    (.put "EMAIL" email)
    (.put "USERNAME" username)
    (.put "PASSWORD" password)
    (.put "CREATED_AT" (now))))

(defn insert-analyze-profile
  [^String profile-id ^String state]
  (doto (KsqlObject.)
    (.put "ID" (.toString (random-uuid)))
    (.put "PROFILE_ID" profile-id)
    (.put "STATE" state)))

(defn insert-files
  [^String profile-id ^String url ^String type]
  (doto (KsqlObject.)
    (.put "ID" (.toString (random-uuid)))
    (.put "PROFILE_ID" profile-id)
    (.put "URL" url)
    (.put "TYPE" type)))

(defn insert-register
  [email username password profile-id]
  (doto (KsqlObject.)
    (.put "ID" (.toString (random-uuid)))
    (.put "EMAIL" email)
    (.put "USERNAME" username)
    (.put "PASSWORD" password)
    (.put "PROFILE_ID" profile-id)
    (.put "CREATED_AT" (now))))

(defn get-profile-data-rows
  []
  {"PROFILE_DATA" (list (insert-profile-data "profile1" "lucas@gmail.com" "lucas" "123")
                    (insert-profile-data "profile2" "ketlen@gmail.com" "ketlen" "123")
                    (insert-profile-data "profile3" "manu@gmail.com" "manu" "123")
                    (insert-profile-data "profile4" "mewtwo@gmail.com" "mewtwo" "123")
                    (insert-profile-data "profile5" "otis@gmail.com" "otis" "123"))})

(defn get-rows-to-insert
  []
  {"ANALYZE_PROFILE" (list (insert-analyze-profile "profile1" "READY")
                       (insert-analyze-profile "profile2" "READY")
                       (insert-analyze-profile "profile3" "READY")
                       (insert-analyze-profile "profile4" "READY")
                       (insert-analyze-profile "profile5" "READY"))
   "FILES"           (list (insert-files "profile1" "www.selfie.com" "selfie")
                       (insert-files "profile2" "www.selfie.com" "selfie")
                       (insert-files "profile3" "www.selfie.com" "selfie")
                       (insert-files "profile4" "www.selfie.com" "selfie")
                       (insert-files "profile5" "www.selfie.com" "selfie"))
   "REGISTER"        (list (insert-register "lucas@gmail.com" "lucas" "123" "profile1")
                       (insert-register "ketlen@gmail.com" "ketlen" "123" "profile2")
                       (insert-register "manu@gmail.com" "manu" "123" "profile3")
                       (insert-register "mewtwo@gmail.com" "mewtwo" "123" "profile4")
                       (insert-register "otis@gmail.com" "otis" "123" "profile5"))})

(defn get-queries-to-exec
  []
  {SELECT_READY_TO_ANALYZE_STMT ["STATE" "ID" "EMAIL" "USERNAME" "PASSWORD" "CREATED_AT"]
   SELECT_FILES_STMT            ["ID" "PROFILE_ID" "URL" "TYPE"]
   SELECT_REGISTER_STMT         ["ID" "EMAIL" "USERNAME" "PASSWORD" "PROFILE_ID" "CREATED_AT"]})
