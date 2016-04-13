(ns eyeota.msgutil
  (:require [clojure.test :as t]
            [clojure.tools.logging :as log]
            [clj-kafka.consumer.zk :as kc]
            [sparkling.destructuring :as s-de]
            [clj-kafka.core :refer :all]
            [kafcas.core :as c]
            [sparkling.core :as s]
            [clojure.edn :as edn]
            [sparkling.kafkautils :as ku]
            [sparkling.stream :as st]
            [sparkling.conf :as conf]
            [sparkling.function :refer [function]]
            [clj-kafka.offset :as offset]
            [clj-kafka.admin :as zkadmin]
            [kafcas.cass :as cas]
            [sparkling.kafkatestutils :as ktu])
  (:use clj-logging-config.log4j)
  (:import [com.eyeota.tsukiji.wahoo.application WahooIntegrationTestUtils]
           [com.eyeota.tsukiji.messaging.transports.kafka KafkaTopicEnum KafkaTransport]
           [java.io File]
           [com.eyeota.tsukiji.messaging.encoders.msgpack MsgpackEncoder]
           [java.util.concurrent Executors ]
           [kafka.serializer DefaultDecoder]
           [com.eyeota.tsukiji.util.time BasicTimeService]
           [com.eyeota.tsukiji.messaging.core MessengerImpl Message AsyncMessageListener]
           [com.eyeota.tsukiji.messaging.componenttests MessagePackAndKafkaComponentTest]
           [com.eyeota.tsukiji.messaging.messages DevProfileSegmentUpdateMessage]))

(defn create-dpsum
  ([] (create-dpsum (.toString (java.util.UUID/randomUUID))))
  ([dpid]
  (doto (DevProfileSegmentUpdateMessage. )
    (.setDeviceProfileId dpid)
    (.setUrl "http://abc.com")
    (.addSegmentId (rand-int 100))
    (.addSegmentId (rand-int 100)))))

;has to be sync'd with test_messenger_properties
(def config {"zookeeper.connect" "localhost:2181"
             "group.id" "wahoo.processors"
             "auto.offset.reset" "smallest"
             "auto.commit.enable" "false"})

(defn get-listener
  [segatom]
  (reify AsyncMessageListener
    (onMessage [this in]
               (let [iseq (seq (.getSegmentNames in))]
                 (try
                   (swap! segatom into iseq)
                   (log/info "kkk segname " iseq  " st " @segatom)
                   (catch Exception e
                     (log/info "kkk gotexcep  " (.getMessage e))))))))


(t/deftest nummsgrecvd
  (t/is (< 1 (count
              (try
    (do (MessagePackAndKafkaComponentTest/setupKafkaBroker)
      (let [ctopicmap {KafkaTopicEnum/TEST_TOPIC1 (int 3)}
            consumer (MessagePackAndKafkaComponentTest/setupConsumer ctopicmap)
            producer (MessagePackAndKafkaComponentTest/setupProducer)
            transport (KafkaTransport. producer consumer)
            ts (BasicTimeService.)
            allsegs (atom [])
            listener (get-listener allsegs)
            threadPool  (Executors/newFixedThreadPool (int 5))]
        (.setCallback consumer transport)
        (let [msngr (MessengerImpl. (int 1) (MsgpackEncoder.) transport threadPool ts nil)]
          (.addAsyncListenerForMessageType msngr listener (class (create-dpsum)))
          (.start consumer)
          (dotimes [i 4]
            (let [imsg (create-dpsum)]
              (log/info "kkksending message " (.getSegmentNames imsg))
            (.cast msngr imsg KafkaTopicEnum/TEST_TOPIC1)))
          (try
            (Thread/sleep 5000)
            (log/info "kkk woke up ")
            (catch Exception e))
          (try
            (.shutdown consumer)
            (catch Exception e))
          (try
            (.shutdown producer)
            (catch Exception e))
            @allsegs
          )))
    (finally
     (try (.stop MessagePackAndKafkaComponentTest/_testCluster) (catch Exception e))))))))



(defn merge-sets
  "merges atomval into pairrdd. Also executes efn to force side effects
  such as writing to another queue, which take the result of collect-map
  on pairrdd"
  ([atomval pairrdd] (merge-sets atomval identity pairrdd))
  ([atomval efn pairrdd]
   (let [res (s/collect-map pairrdd)
         efres (efn res)]
     (swap! atomval (partial merge-with into) res))))

(defn decodemsgp
  [x]
  (.decode (MsgpackEncoder.) (s-de/value x)))

(defn segnames
  [x]
  (.getSegmentNames x))

(defn devprofid
  [x]
  (.getDeviceProfileId x))

(defn peekfn
  [x]
   (try
      (log/info "kkkpeek " (devprofid x) " s " (segnames x))
      (catch Exception e ))
    x)

(defn efn
  [x]
  (println "kkk efn " x))

(defn xfrmfn
  [ks]
  (try
    (->> ks
         (s/map decodemsgp)
         ;key is a string-used for partitioning between brokers
         ;make it a pair by specifying the key
         ;(s/map peekfn)
         (s/key-by devprofid)
         (s/map-values segnames)
         (s/map-values set)
         ;need to reduce since this is sent as a batch, not
         ;as separate messages
         (s/reduce-by-key into))
    (catch Exception e
      (println "kkk got exception " (.getMessage e) " class " (class ks)))))

(t/with-test
  (defn kaftest2
    "send DPSUM messages into Kafka and read it using Spark's Kafka integration"
    [{:keys [msgs xfrmfn foreachrddfn result] :as m}]
    (try
      (s/with-context sc c/testconf
        (let [ssc (st/streaming-context sc 500)
              _ (MessagePackAndKafkaComponentTest/setupKafkaBroker)
              producer (MessagePackAndKafkaComponentTest/setupProducer)
              transport (KafkaTransport. producer nil)
              ts (BasicTimeService.)
              threadPool  (Executors/newFixedThreadPool (int 5))
              topics {(.getValue KafkaTopicEnum/TEST_TOPIC1) (int 1)}
              ks (ku/create-msgpack-string ssc (assoc config "zookeeper.connect"
                                                 MessagePackAndKafkaComponentTest/_zkConnectString)
                                           topics)
              msngr (MessengerImpl. (int 1) (MsgpackEncoder.) transport threadPool ts nil)]
          (try
            (log/info "kkk setup done " MessagePackAndKafkaComponentTest/_brokerString)
            (doseq [i msgs]
              (log/info " kkk sending " (.getSegmentNames i) " " (.getDeviceProfileId i))
              (.cast msngr i KafkaTopicEnum/TEST_TOPIC1))
            (st/foreachrdd (xfrmfn ks) foreachrddfn)
            (st/start ssc)
            (let [startTime  (System/currentTimeMillis)]
              (while ;(and (not= (count @result) 3)
                (> 5000 (- (System/currentTimeMillis) startTime))
                (Thread/sleep 200)))
            (deref result)
            (finally
             (try
               (st/stop ssc)
               (catch Exception e))))))
      (catch Throwable t
        #_(.printStackTrace t))))
  (let [ msgs (for [i (range 5) u ["uid1" "uid2"]]
                (create-dpsum u))
         result (atom {})
         kfn (partial merge-sets result efn)
         retres (kaftest2 {:result result :msgs msgs
                           :xfrmfn xfrmfn :foreachrddfn kfn})]
    (t/is (every? #(> % 6) (map count (vals retres))))))

(defn topic-handler
  [topic handler]
  (fn [{:keys [ssc zkconn numthreads ] :or {numthreads (int 1)} :as m}]
    (try
    (let [topics {topic numthreads}
          ks (ku/create-msgpack-string ssc
                                       (assoc config "zookeeper.connect" zkconn)
                                       topics)]
      (log/info " kkk execing topic-handler " )
      (handler (assoc m :ks ks)))
      (catch Exception e
        (log/error " kkk topic-handler exception " (.getMessage e))))))

(defn mk-topic-handler
  [result]
  (let [ kfn (partial merge-sets result efn)
         handlerfn
         (fn [{:keys [ks]}]
           (do (log/info " kkk in topic handler ")
             (st/foreachrdd (xfrmfn ks) kfn)))]
    handlerfn))

(defn send-msg-handler
  [msgs topic]
  (fn [{:keys [msngr]}]
    (doseq [i msgs]
      (log/info " kkk sending " (.getSegmentNames i) " " (.getDeviceProfileId i))
      (.cast msngr i topic))))

(t/with-test
  (defn kaftest3
    "register multiple handlers that operate on one topic "
    [{:keys [handlers tconf] :as m}]
    (try
      (s/with-context sc tconf
        (let [ssc (st/streaming-context sc 500)
              _ (MessagePackAndKafkaComponentTest/setupKafkaBroker)
              producer (MessagePackAndKafkaComponentTest/setupProducer)
              transport (KafkaTransport. producer nil)
              ts (BasicTimeService.)
              threadPool  (Executors/newFixedThreadPool (int 5))
              msngr (MessengerImpl. (int 1) (MsgpackEncoder.) transport threadPool ts nil)
              m (merge m {:ssc ssc :zkconn MessagePackAndKafkaComponentTest/_zkConnectString
                 :msngr msngr :sc (.sc ssc)})]
          (try
            (log/info "kkk setup done " MessagePackAndKafkaComponentTest/_brokerString)
            (doseq [h handlers] (h m))
            (st/start ssc)
            (let [startTime  (System/currentTimeMillis)]
              (while ;(and (not= (count @result) 3)
                (> 5000 (- (System/currentTimeMillis) startTime))
                (Thread/sleep 200)))
            (finally
             (try
               (st/stop ssc)
               (catch Exception e))))))
      (catch Throwable t
        (.printStackTrace t)
        (log/error "kkk got except " (.getMessage t)))))
  (let [result (atom {})
        msgs (for [i (range 5) u ["uid1" "uid2"]]
               (create-dpsum u))
        testtop (.getValue KafkaTopicEnum/TEST_TOPIC1)
        sendh (send-msg-handler msgs KafkaTopicEnum/TEST_TOPIC1)
        recvh (topic-handler testtop (mk-topic-handler result))
        handlers [sendh recvh]]
    (kaftest3 {:handlers handlers :tconf c/testconf})
    (t/is (= ["uid1" "uid2"]
             (keys (deref result))))))

(def kaf-cas-conf
  (-> (conf/spark-conf)
      (conf/set "spark.cassandra.connection.host" "127.0.0.1")
      (conf/set "spark.cassandra.connection.port" "9142")
      (conf/master "local[*]")
      (conf/app-name "core-test")))

(defn kaf-cass-env
  "register multiple handlers that operate in a Kafka+Cassandra env"
  [{:keys [before-afters] :as m}]
  (let [fnx (fn [] (kaftest3 m))]
        (cas/cassba before-afters fnx)))

(defn scoring-fn
  [x]
  (double 0.1))

(defn merge-segs
  "merges atomval into pairrdd. Also executes efn to force side effects
  such as writing to another queue, which take the result of collect-map
  on pairrdd"
  [atomval efn imap pairrdd ]
  (let [res (cas/joinonrdd (assoc imap :keyseq (.rdd pairrdd)))
        efres (efn res)
    scored (apply merge (doseq [[k v] res] {k {:seg v :score (scoring-fn v)}}))]
    ))

(defn merge-segs
  "merges atomval into pairrdd. Also executes efn to force side effects
  such as writing to another queue, which take the result of collect-map
  on pairrdd"
  ([atomval efn imap pairrdd ]
   (let [res (cas/joinonrdd (assoc imap :keyseq (.rdd pairrdd)))
         efres (efn res)]
     (swap! atomval (partial merge-with into) res))))

(defn segstrnames
  "returns a set of segment names as strings "
  [x]
  (set (map str (seq (.getSegmentNames x)))))

(defn xfrmfn2
  [ks]
  (try
    (->> ks
         (s/map decodemsgp)
         ;key is a string-used for partitioning between brokers
         ;make it a pair by specifying the key
         (s/key-by devprofid)
         (s/map-values segstrnames)
         ;need to reduce since this is sent as a batch, not
         ;as separate messages
         (s/reduce-by-key into))
    (catch Exception e
      (println "kkk got exception " (.getMessage e) " class " (class ks)))))

(defn mk-cass-handler
  [result]
  (let [kfn (partial merge-segs result efn)
        handlerfn
         (fn [{:keys [ks] :as m}]
           (do (log/info " kkk in topic handler ")
             (st/foreachrdd (xfrmfn2 ks) (partial kfn m))))]
    handlerfn))

(t/deftest segment-join
  ;sends out messages on kafka
  ;the kafka util receives it, parses it
  ;does a join with cassandra to retrieve the past segments
  (let [result (atom {})
        msgs (for [i (range 5) u ["a1" "a2"]]
               (create-dpsum u))
        ;keyseq keyspace tablename joincol
        imap {:keyspace "testks" :tablename "seg" :joincol "id" }
        ba {:before cas/before-seg :after cas/after-seg}
        testtop (.getValue KafkaTopicEnum/TEST_TOPIC1)
        sendh (send-msg-handler msgs KafkaTopicEnum/TEST_TOPIC1)
        recvh (topic-handler testtop (mk-cass-handler result))
        handlers [sendh recvh]
        _ (kaf-cass-env (merge imap {:handlers handlers :tconf kaf-cas-conf :before-afters ba}))
        res (deref result)
        expkeys #{"a1" "a2"}
        joinvals {"a1" #{"s1" "s2"} "a2" #{"s2" "s3"}}
        kres (keys res)]
    (t/is (apply = (map count [kres expkeys])))
    (t/is (every? not-empty (map #(clojure.set/intersection %1 %2)
                                 (map joinvals expkeys) (map res expkeys))))))
