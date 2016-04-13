(ns sparkling.stream
  (:require [sparkling.conf :as conf]
            [sparkling.core :as s]
            [sparkling.destructuring :as s-de]
            [sparkling.function :refer [void-function]])
  (:import [org.apache.spark.streaming.api.java JavaStreamingContext]
           [org.apache.spark.streaming Durations Duration Time State StateSpec])
  (:gen-class))

(defn streaming-context
  ([sc] (streaming-context sc 500))
  ([sc dur]
    (JavaStreamingContext. sc (Duration. dur))))

(defn start
  [stc]
  (.start stc))

(defn stop
  [stc]
  (.stop stc))

(defn foreachrdd
  [dstream foreachfn]
  (.foreachRDD dstream (void-function foreachfn)))
