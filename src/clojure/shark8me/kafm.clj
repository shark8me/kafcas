(ns shark8me.kafm
  (:require [clojure.test :as t]
            [clojure.tools.logging :as log]
            [clj-kafka.consumer.zk :as kc]
            [clj-kafka.consumer.simple :as kcs]
            [sparkling.destructuring :as s-de]
            [clj-kafka.core :refer :all]
            [clj-kafka.test.utils :as cktu]
            [kafcas.core :as c]
            [sparkling.core :as s]
            [clojure.edn :as edn]
            [sparkling.kafkautils :as ku]
            [sparkling.stream :as st]
            [sparkling.conf :as conf]
            [sparkling.function :refer [function]]
            [clj-kafka.offset :as offset]
            [clj-kafka.producer :as pro]
            [clj-kafka.admin :as zkadmin]
            [clj-kafka.test.consumer.zk :as zkc]
            [kafcas.cass :as cas]
            [sparkling.kafkatestutils :as ktu])
  (:use clj-logging-config.log4j)
  (:import  [java.io File]
            [java.util.concurrent Executors ]
           [kafka.serializer DefaultDecoder]))

(def test-broker-config {:zookeeper-port 2182
                         :kafka-port 9999
                         :topic "test"})

(def consumer-config {"zookeeper.connect" "localhost:2182"
             "group.id" "clj-kafka.consumer"
             "auto.offset.reset" "smallest"
             "auto.commit.enable" "false"})
(def producer-config {"metadata.broker.list" "localhost:9999"
                     "serializer.class" "kafka.serializer.DefaultEncoder"
                     "partitioner.class" "kafka.producer.DefaultPartitioner"})
(defn make-msg
  [i]
  (pro/message "test" (.getBytes (str (str i) "this is my message"))))

(defn get-val
  [m]
  (String. (:value m)))

(defn runt2
  []
(cktu/with-test-broker test-broker-config
  (with-resource [c (kc/consumer consumer-config)]
    kc/shutdown
    (let [p (pro/producer producer-config)]
      (pro/send-messages p (mapv make-msg (range 10))))
    (let [ic (kcs/consumer "127.0.0.1" 9999 "simple-consumer")
          ct (System/currentTimeMillis)
          msgst (kc/messages c "test")]
      (while (> 10000 (- (System/currentTimeMillis) ct))
        (println "kkk loop " (- (System/currentTimeMillis) ct))
        (try
          (println "kkk " (kcs/latest-topic-offset ic "test" 0))
          #_(println "kkkk count " (->> (kc/messages c "test")
                                      (map :value)
                                      (map #(String. %))
                                      count))
          (println "kkk f " (get-val (first msgst)))
          (Thread/sleep 200)
          (catch Exception e (println "kkk excep " (.getMessage e)))
          ))))))

;(runt2)


