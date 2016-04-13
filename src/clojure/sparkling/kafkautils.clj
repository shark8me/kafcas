(ns sparkling.kafkautils
  (:require [sparkling.conf :as conf]
            [sparkling.core :as s]
            [sparkling.destructuring :as s-de]
            [sparkling.function :refer [void-function]])
  (:import [org.apache.spark.streaming.api.java JavaStreamingContext]
           [org.apache.spark.streaming.kafka KafkaUtils]
           [kafka.serializer StringDecoder DefaultDecoder]
           [org.apache.spark.streaming Durations Duration Time State StateSpec])
  (:gen-class))

(defn create-stream-string
  [ssc kfp topics]
  (KafkaUtils/createStream ssc String
                           String
                           StringDecoder
                           StringDecoder
                           kfp
                           topics
                           (s/STORAGE-LEVELS :memory-only)))

(defn create-msgpack-string
  [ssc kfp topics]
  (KafkaUtils/createStream ssc String
                           String
                           StringDecoder
                           DefaultDecoder
                           kfp
                           topics
                           (s/STORAGE-LEVELS :memory-only)))
