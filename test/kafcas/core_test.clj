(ns kafcas.core-test
  (:require [sparkling.serialization]
            [kafcas.core :as c]
            [sparkling.conf :as conf]
            [sparkling.core :as s]
            [sparkling.destructuring :as s-de]
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
            [clojure.test :as t]
            [clj-kafka.consumer.zk :refer [messages]])
  (:import [org.apache.spark.api.java JavaRDD JavaPairRDD JavaSparkContext]
           [java.util HashMap Collections]
           [org.apache.spark.streaming.api.java JavaStreamingContext]
           [com.datastax.spark.connector.japi SparkContextJavaFunctions CassandraJavaUtil]
           [org.apache.spark.streaming Durations Duration Time State StateSpec]
           [org.apache.spark.streaming.kafka KafkaUtils KafkaTestUtils]
           [kafka.serializer StringDecoder]
           [org.apache.spark.mllib.util MLUtils])
  (:gen-class))

(defn addvals
  [hm pairrdd]
  (let [res (s/collect-map pairrdd) ]
    (doseq [k (keys res) ]
      (let [v (get res k)
            re (swap! hm assoc k (+ v (get @hm k 0)))]))))

(defn minto
  [hm pairrdd]
  (let [res (s/collect-map pairrdd) ]
    (swap! hm (partial merge-with into) res)))

(comment
(defn mmerge
  [hm pairrdd]
  (let [res (s/collect-map pairrdd) ]
    (swap! hm merge res))))

(t/deftest defaulttest
  (let [ topic "test-topic"
         topics {topic (int 1)}
         sent (zipmap ["a" "b" "c"] (map int (range 1 4)))
         result (atom {})
         kfn (partial addvals result)
         xfrmfn (fn [ks] (.countByValue (.map ks (function s-de/value))))
         foreachrddfn kfn ]
    (t/is (= sent
             (c/kaftest {:topic topic :topics topics
                         :sent sent :result result
                         :kfn kfn :xfrmfn xfrmfn :foreachrddfn kfn})))))
(t/deftest simple
  ;the simplest possible test

  (let [ m1 [{:a 2 :seg #{1 2}} {:a 4 :seg #{3 5}}]
         expected (apply merge (map #(assoc {} (:a %) (:seg %)) m1))
         topic "test-topic"
         sent (into-array (map pr-str m1))
         topics {topic (int 1)}
         result (atom {})
         kfn (partial c/mmerge result) ;just merge the previous and current maps
         xfrmfn (fn [ks] (->> ks ;this is a pair rdd, where the key is nil (dont know why)
                              (s/map s-de/value) ;exxtract the value part of the tuple
                              (s/map edn/read-string) ;convert it back to a clojure map
                              (s/key-by :a) ;make :a the key
                              (s/map-values :seg))) ;make the :seg part as the value
         foreachrddfn kfn]
    (t/is (= expected
             (c/kaftest {:topic topic :topics topics
                         :sent sent :result result
                         :kfn kfn :xfrmfn xfrmfn :foreachrddfn kfn})))))

(defn decoratevals
  [imap]
  (let [incfn (fn[s] (set (map inc s)))]
    (apply merge (map (fn[[k v]] (assoc {} k (incfn v))) imap))))

(defn spitkaf
  [ktu topic res]
  (let [sent (into-array (map pr-str (decoratevals res)))]
   (.sendMessages ktu topic sent)))



(defn mkforeach
  "returns a function that calls foreachRDD on the stream,
  with the transform fn and the foreach fn"
  [xfrmfn vfn]
  (fn [ks]
   (.foreachRDD (xfrmfn ks) (void-function vfn))))

(t/deftest two_queues
  ;this test, we read a map from one queue, decorate the data (for e.g.
  ;add some information from cassandra), send it to another queue,
  ;have another stream read from that queue and save it.

  ;first queue input: [{:a 2 :seg #{1 2}} {:a 4 :seg #{3 5}}]
  ;first queue post processing: {2 #{1 2} 4 #{ 3 5}}
  ;decoration: increment each val in the set
  ;second queue output: {2 #{3 2} 4 #{ 4 6}}
  (let [ topic1 "topic1"
         topic2 "topic2"
         ktu (doto (KafkaTestUtils.) (.setup))
         sent (into-array (map pr-str [{:a 2 :seg #{1 2}} {:a 4 :seg #{3 5}}]))
         result1 (atom {})
         result2 (atom {})
         skfn (partial spitkaf ktu topic2)
         kfn1 (partial c/mmerge result1 skfn)
         kfn2 (partial c/mmerge result2)
         stopfn (fn [] (not= (count (keys (deref result2))) 2))
         xfrmfn (fn [ks] (->> ks c/unwrap
                              (s/key-by :a)
                              (s/map-values :seg)))
         xfrmfn2 (fn [ks] (->> ks c/unwrap
                               (s/key-by first)
                               (s/map-values second)))
         foreachfns (mapv mkforeach [xfrmfn xfrmfn2] [kfn1 kfn2])]
    (doseq [i [topic1 topic2]] (.createTopic ktu i))
    (.sendMessages ktu topic1 sent)

    (c/kafmulqtest {:topics [{topic1 (int 1)} {topic2 (int 2)}]
                    :ktui ktu :foreachfns foreachfns :stopfn stopfn})
    (t/is (= {2 #{1 2} 4 #{ 3 5}} (deref result1)))
    (t/is (= {2 #{2 3} 4 #{ 4 6}} (deref result2)))))

(comment

(let [ topic "test-topic"
          ktu (doto (KafkaTestUtils.) (.setup))
         sent (into-array (map pr-str [{:a 2 :seg #{1 2}} {:a 4 :seg #{3 5}}]))
         topics {topic (int 1)}
         result (atom {1 #{2 4}})
         kfn (partial c/mmerge result)
         xfrmfn (fn [ks] (->> ks (s/map s-de/value)
                              (s/map edn/read-string)
                              (s/key-by :a)
                              (s/map-values :seg)))
         foreachrddfn kfn]
        (.createTopic ktu topic)
        (.sendMessages ktu topic sent)
 (c/kaftest {:topics topics
                         :sent sent :result result
                         :kfn kfn :xfrmfn xfrmfn :foreachrddfn kfn}))

  (s/with-context sc c/testconf
      (let [m1 [[1 2] [3 4]]
            m2 [[nil {:a 2}] [nil {:a 3}]]
            rdd (s/into-pair-rdd sc (map (fn[[x y]] (s/tuple x y )) m2 ))]
        ;(->> rdd (s/map-values :a)  s/collect-map)
        (->> rdd (s/map s-de/value) (s/key-by :a ) s/collect-map)
        ))

  (s/with-context
    sc (-> (conf/spark-conf) (conf/master "local[*]") (conf/app-name "abc"))
    (->> (s/into-rdd sc (range 1 10))
         (s/map inc)
         (s/collect)))
  )
