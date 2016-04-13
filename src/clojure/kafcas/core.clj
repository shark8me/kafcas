(ns kafcas.core
  (:require [sparkling.serialization]
            [sparkling.conf :as conf]
            [sparkling.broadcast :as bcast]
            [sparkling.core :as s]
            [sparkling.destructuring :as s-de]
            [sparkling.stream :as st]
            [sparkling.kafkautils :as ku]
            [sparkling.kafkatestutils :as ktu]
            [clojure.edn :as edn]
            [sparkling.function :refer [flat-map-function
                                        flat-map-function2
                                        function
                                        function2
                                        function3
                                        pair-function
                                        pair-flat-map-function
                                        void-function]]
            [embedded-kafka.core :refer [with-test-broker]]
            [clj-kafka.producer :refer [send-message message]]
            [clj-kafka.consumer.zk :refer [messages]])
  (:import [org.apache.spark.api.java JavaRDD JavaPairRDD JavaSparkContext]
           [java.util HashMap Collections]
           [org.apache.spark.streaming.api.java JavaStreamingContext]
;           [com.datastax.spark.connector.japi SparkContextJavaFunctions CassandraJavaUtil]
           [org.apache.spark.streaming Durations Duration Time State StateSpec]
           [org.apache.spark.streaming.kafka KafkaUtils KafkaTestUtils]
           [kafka.serializer StringDecoder]
           [org.apache.spark.mllib.util MLUtils])
  (:gen-class)
  )

(def testconf
  (-> (conf/spark-conf)
      (conf/set "spark.cassandra.connection.host" "127.0.0.1")
;      (conf/set "spark.kryo.registrationRequired" "false")
      (conf/master "local[*]")
      (conf/app-name "core-test")))


(defn mmerge
  "merges atomval into pairrdd. Also executes efn to force side effects
  such as writing to another queue, which take the result of collect-map
  on pairrdd"
  ([atomval pairrdd] (mmerge atomval identity pairrdd))
  ([atomval efn pairrdd]
   (let [res (s/collect-map pairrdd)
         efres (efn res)]
     (swap! atomval merge res))))

(defn voidfnatom
  [hm pairrdd]
  (let [res (s/collect-map pairrdd) ]
;    (println " resultkkk " res " hm " @hm  " keys " (keys res) )
    (doseq [k (keys res) ]
      (let [v (get res k)
          re (swap! hm assoc k (+ v (get @hm k 0)))]
;        (println " resultkkk1 " @hm )
      ))))

(defn kaftest
  [{:keys [sent topics topic xfrmfn foreachrddfn result] :as m}]
  (try
  (s/with-context sc testconf
    (let [ssc (st/streaming-context sc 500)
          ktui (ktu/create-testutil)
          kfp {"zookeeper.connect" (.zkAddress ktui)
               "group.id" "consumer"
               "auto.offset.reset" "smallest"}
          ks (ku/create-stream-string ssc kfp topics)]
      (try
        (println "setup done " )
        (ktu/create-topic ktui topic)
        (ktu/send-messages ktui topic sent)
        (st/foreachrdd (xfrmfn ks) foreachrddfn)
        (st/start ssc)
        (let [startTime  (System/currentTimeMillis)]
          (while (and (not= (count @result) 3)
                      (> 5000 (- (System/currentTimeMillis) startTime)))
            (Thread/sleep 200)))
        (deref result)
        (finally
         (try
         (ktu/teardown ktui)
           (catch Exception e))
         (try
         (st/stop ssc)
           (catch Exception e))
         ))))
  (catch Throwable t
    #_(.printStackTrace t))))

(defn mkstream
  "returns a DStream from a Kafka queue"
  [{:keys [topics ktui ssc] :as m}]
  (let [kfp {"zookeeper.connect" (.zkAddress ktui)
             "group.id" "consumer"
             "auto.offset.reset" "smallest"}]
    (ku/create-stream-string ssc kfp topics)))

(defn kafmulqtest
  "test utility that can consume from multiple topics"
  [{:keys [topics foreachfns ktui timeout stopfn]
    :or {timeout 10000} :as m}]
  (try
  (s/with-context sc testconf
    (let [ssc (st/streaming-context sc 500)
          ;create one stream for each topic
          ks (mapv #(mkstream (assoc m :ssc ssc :topics %)) topics )]
      (try
        (println "setup done " )
        ;set up transformers to run on each stream/rdd
        (doseq [[f k] (mapv vector foreachfns ks)] (f k))

        (st/start ssc)
        (let [startTime  (System/currentTimeMillis)]
          (while (and (stopfn)
                      (> 10000 (- (System/currentTimeMillis) startTime)))
            (Thread/sleep 200)))
        (finally
         (ktu/teardown ktui)
         (st/stop ssc)))))
  (catch Throwable t
    #_(.printStackTrace t))))

(defn unwrap
  [ks]
  (->> ks
       (s/map s-de/value)
       (s/map edn/read-string)))

  (comment

(s/with-context
  sc
  testconf
  (let [rd (.cassandraTable (CassandraJavaUtil/javaFunctions sc) "test" "kv")]
    (s/first rd)))
  (->> (messages consumer "test-topic")
      (map :value)
      (map #(String. %) )
      )


  )


