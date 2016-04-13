(ns sparkling.kafkatestutils
  (:require [sparkling.conf :as conf]
            [sparkling.core :as s]
            [sparkling.destructuring :as s-de]
            [sparkling.function :refer [void-function]])
  (:import [org.apache.spark.streaming.api.java JavaStreamingContext]
           [org.apache.spark.streaming.kafka KafkaTestUtils]
           [org.apache.spark.streaming Durations Duration Time State StateSpec])
  (:gen-class))

(defn create-testutil
  []
  (doto (KafkaTestUtils.) (.setup)))

(defn create-topic
  [ktu topic]
  (.createTopic ktu topic))

(defn send-messages
  [ktu topic sent]
  (.sendMessages ktu topic sent))

(defn teardown
  [ktu]
  (.teardown ktu))
