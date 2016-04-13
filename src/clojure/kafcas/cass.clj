(ns kafcas.cass
  (:require [sparkling.conf :as conf]
            [sparkling.core :as s]
            ;[kafcas.core :as kc]
            [sparkling.destructuring :as s-de]
            [clojure.java.io :as io]
            [clojure.set :as cset]
            [qbits.alia :as alia]
            [qbits.hayt :as ht]
            [clojure.tools.logging :as log]
            [vinyasa.reflection :refer :all]
            [clojure.test :as t])
  (:use clj-logging-config.log4j)
  (:import [org.apache.spark.api.java JavaRDD JavaPairRDD JavaSparkContext]
           [java.util HashMap Collections]
           [java.io File]
           [com.datastax.spark.connector.util JavaApiHelper]
           [eyeota DevProfile Stest]
           [scala.collection JavaConversions]
           [scala Tuple1 Tuple3]
           [com.datastax.spark.connector.japi CassandraJavaUtil RDDJavaFunctions]
           [com.datastax.spark.connector.types TypeConverter$StringConverter$]
           [org.cassandraunit.utils EmbeddedCassandraServerHelper]
           [org.apache.spark.streaming.api.java JavaStreamingContext])
 (:gen-class))

(def ^:dynamic *session*)
(def ^:dynamic *cluster*)

(defn embedded-cassandra [f]
  (do
  (EmbeddedCassandraServerHelper/startEmbeddedCassandra 40000)
  (binding [*cluster* (alia/cluster {:contact-points ["localhost"] :port 9142})]
    (binding[ *session* (alia/connect *cluster*)]
    (try
      (f)
      (finally
        (alia/shutdown *session*)
        (alia/shutdown *cluster*)
        (EmbeddedCassandraServerHelper/cleanEmbeddedCassandra)))))))

(def before-seg
  ["CREATE KEYSPACE testks WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};"
   "USE testks;"
   "CREATE TABLE seg ( id varchar, segset set<varchar>, PRIMARY KEY (id));"
   "INSERT INTO seg (id, segset) VALUES ('a1',{'s1', 's2'})"
   "INSERT INTO seg (id, segset) VALUES ('a2',{'s2', 's3'})"])


(def before-simple-seg
  ["CREATE KEYSPACE testks WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};"
   "USE testks;"
   "CREATE TABLE seg ( id varchar, segset varchar, PRIMARY KEY (id));"
   "INSERT INTO seg (id, segset) VALUES ('a1','s1')"
   "INSERT INTO seg (id, segset) VALUES ('a2','s1')"
   "INSERT INTO seg (id, segset) VALUES ('a3','s1')"
   "INSERT INTO seg (id, segset) VALUES ('a4','s1')"])

(def before-simple-seg2
  ["CREATE KEYSPACE testks WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};"
   "USE testks;"
   "CREATE TABLE seg ( id varchar, segset varchar, PRIMARY KEY (id));"
   "CREATE TABLE seg2 ( id varchar, segset varchar, PRIMARY KEY (id));"
   "INSERT INTO seg (id, segset) VALUES ('a1','s1')"
   "INSERT INTO seg (id, segset) VALUES ('a2','s1')"
   "INSERT INTO seg2 (id, segset) VALUES ('a1','s2')"
   "INSERT INTO seg2 (id, segset) VALUES ('a2','s3')"])

(def before-int-key
  ["CREATE KEYSPACE testks WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};"
   "USE testks;"
   "CREATE TABLE seg ( id int, segset varchar, PRIMARY KEY (id));"
   "INSERT INTO seg (id, segset) VALUES (1,'s1')"
   "INSERT INTO seg (id, segset) VALUES (2,'s3')"])

(def before-score-seg
  ["CREATE KEYSPACE testks WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};"
   "USE testks;"
   "CREATE TABLE seg ( id varchar, segset set<varchar>, score double,  PRIMARY KEY (id));"
   "INSERT INTO seg (id, segset) VALUES ('a1',{'s1', 's2'})"
   "INSERT INTO seg (id, segset) VALUES ('a2',{'s2', 's3'})"])

(def after-seg
  ["DROP KEYSPACE testks;"])

(defn setup-cassandra
  [{:keys [before after]} f]
  (try
    (doseq [i before]
      (alia/execute *session* i))
    (f)
    (finally
     (try (doseq [i before]
            (alia/execute *session* i))
       (catch Exception _ nil)))))

(defn cally
  []
  (alia/execute *session* "select * from seg;"))

(def testconf
  (-> (conf/spark-conf)
      (conf/set "spark.cassandra.connection.host" "127.0.0.1")
      (conf/set "spark.cassandra.connection.port" "9142")
      (conf/set "spark.kryo.registrationRequired" "false")
      (conf/master "local[*]")
      (conf/app-name "core-test")))

(defn cassba
  "call function wrapped with cassandra before-afters."
  [imap fnx]
  (let [snx (partial setup-cassandra imap (partial fnx))]
    (embedded-cassandra snx)))

(defn wrap-cass
  ([fnx] (wrap-cass {:before before-seg :after after-seg} fnx))
  ([imap fnx]
   (s/with-context
     sc testconf
     (cassba imap (partial fnx sc)))))

(defn get3tup
  [x]
  (do (println " kkk 3tup " x)
  (.toMap x)))

(defn update-in-flip
  [ks fnx m]
  (update-in m ks fnx))

(defn update-flip
  [ks f m]
  (update m ks f))

(defn asjava
   [x]
   (java.util.HashSet. (JavaConversions/setAsJavaSet x)))

(defn assoc-flip
  [m]
  (try
  (let [res {}]
        ;(assoc {} :segset (asjava (get m "segset")) :id (get m "id")) ]
    ;(println "kkk res " (get m "segset") " res " res)
    m)
    (catch Exception e
      (println " kkk ex " (.getMessage e))
      m)))
(defn getk
  [x]
  (do (println "kkk getk " x)
  (get x "segset")))

(defn getstringkey
  [k x]
  (get x k))

(defn peekabcd
  [x]
  [x "2"])

(defn peekabc
  [x]
  [x "2"])

(defn swapargs
  [f k m]
  (f m k))

(defn tocljmap
  [x]
  (let [m (.toMap x)
        m1 (into {} m)
        sseg (java.util.HashSet. (JavaConversions/setAsJavaSet (get m1 "segset")))
        id (get m1 "id")]
    sseg))

(comment
(->> rd
           (s/map get3tup)
           (s/map (partial into {}))
           (s/map getk)
           (s/map asjava)
           ))
(t/with-test
  (defn getseg
    "returns the segments associated with this id"
    [{:keys [keyspace tablename where-key where-col sc sel-col]}]
    (let [rd (-> (CassandraJavaUtil/javaFunctions sc)
                 (.cassandraTable  keyspace tablename)
                 (.where (str where-col " = ?") (into-array String [where-key]))
                 (.select (into-array String sel-col))) ]
      (->> rd
           (s/map get3tup)
           (s/map (partial into {}))
           (s/key-by (partial getstringkey "id"))
           (s/map-values (partial swapargs select-keys ["score" "segset"]))
           ;(s/map-values getk)
           ;(s/map-values asjava)
           )))
  (let [imap {:keyspace "testks" :tablename "seg" :where-col "id" :where-key "a1"
              :sel-col ["id","score","segset"]}]
    (s/with-context
      sc testconf
      (cassba {:before before-score-seg :after after-seg}
                 (fn [] (s/collect-map (getseg (assoc imap :sc sc ))))))))

(t/with-test
  (defn tabfirst
    [sc]
    (let [rd (.cassandraTable (CassandraJavaUtil/javaFunctions sc) "testks" "seg")]
      (->  (.where rd "id = ?" (into-array String ["a1"]))
           (.select (into-array String ["segset"]))
           s/first
           (.getSet "segset"))))
  (t/is (= #{"s1" "s2"}
           (wrap-cass {:before before-seg :after after-seg} tabfirst))))

(t/deftest simple-set-get
  (s/with-context
    sc testconf
    (let [[ids segs] [(map str (range 4)) (map str (range 4 8))]
          parr (mapv s-de/tuple ids segs)
          rdd (s/into-rdd sc parr)
          cst (fn [] (-> (CassandraJavaUtil/javaFunctions rdd)
                         (.writerBuilder  "testks" "seg"
                                          (CassandraJavaUtil/mapTupleToRow (class "") (class "")))
                         (.saveToCassandra)))
          fnx2a (fn[] (let [rd (-> (.cassandraTable (CassandraJavaUtil/javaFunctions sc)
                                                    "testks" "seg"
                                                    (CassandraJavaUtil/mapRowToTuple (class "") (class ""))))]
                        (->> rd
                             (s/map (s-de/key-value-fn vector))
                             s/collect)))
          fnx3 (fn[] (do (cst) (fnx2a)))
      res (cassba {:before before-simple-seg :after after-seg} fnx3)
          intercnt (fn[ s1 s2]
                     (count (cset/intersection (set s1) (set s2))))]
      (t/is (= (intercnt ids (map first res)) (count ids)))
      (t/is (= (intercnt segs (map second res)) (count segs))))))

(t/with-test
  (defn persist-score
    "persist the score assigned to each cookie "
    [{:keys [keyseq keyspace tablename sc]}]
    (let [rdd (.rdd (s/into-rdd sc (map (fn [[k v]] (Tuple3. k (:score v) (:seg v))) keyseq)))]
      (-> (CassandraJavaUtil/javaFunctions rdd)
          (.writerBuilder keyspace tablename
                          (CassandraJavaUtil/mapTupleToRow
                           String java.util.Set Double ))
          (.saveToCassandra))))

  (s/with-context
    sc testconf
    (let [kseq {"a1" {:seg #{"s1"} :score (double 0.1)} "a2" {:seg #{"s2"} :score (double 0.2)}}
          imap {:keyspace "testks" :tablename "seg" :sc sc
                :keyseq kseq}
          imapres {:keyspace "testks" :tablename "seg" :where-col "id"
                   :sc sc :sel-col ["id","score","segset"]}
          get-score-fn (fn[id] (-> (getseg (assoc imapres :where-key id))
                         s/collect-map
                                    (get "id")))
          fnx3 (fn[] (do (persist-score imap)
                           (doseq [i (keys kseq)] ((get-score-fn i)))))

          res (cassba {:before before-score-seg :after after-seg} fnx3)]
       res)))

(defn getset
  [x]
  (.getSet x 1))

(t/with-test
  (defn make-where-key
    [where-val]
    (let [cnt (dec (count where-val))]
      (str "(" (clojure.string/join (for [i (range cnt)] "?,")) "?)" )))
  (t/is (= "(?,?,?)" (make-where-key [10 20 30]))))

;doesn't work, where clause in fails.
(defn get-table-in
  [{:keys [keyspace tablename sc where-key where-val sel-cols]}]
  (let [rd (->(.cassandraTable (CassandraJavaUtil/javaFunctions sc) keyspace tablename)
              ;(.where (str where-key (make-where-key where-val)) (into-array String where-val))
              (.where "id in (? ,?)" (into-array String ["a1" "a2"]))
              ;(.select (into-array String sel-cols))
              )]
    (->> rd
         (s/map getset)
         (s/collect))))

(t/with-test
  (defn get-table-hayt
    "returns the result of querying a  table with where-in"
    [{:keys [tablename where-key where-val ]}]
    (alia/execute *session*
                  (ht/select tablename (ht/where [[:in where-key where-val]]))))
  (let [bamap  {:before before-seg :after after-seg}
        expres '({:id "a1", :segset #{"s1" "s2"}} {:id "a2", :segset #{"s3" "s2"}})
        fnargs {:tablename "seg"
                :where-key :id :where-val ["a1" "a2"]}]
    (t/is (= expres
             (cassba bamap (partial get-table-hayt fnargs))))))

(defn getx
  [k v x]
  (vector (.getString x k) (.getString x v)))

(defn noop
  [x]
  (try
    (println " kkk mtype " (str (.? x "getSet" :#)))
    (println "kkk noop " (.getSet x 0 TypeConverter$StringConverter$))
    (catch Throwable t
      (println "kkk err " (.getMessage t))))
  x)

(defn get-string-set
  "returns key as string and value as set"
  [k v x]
  (try
    (println " kkk mtype " (str (.? x "getSet" :#)))
    (finally
  (vector (.getString x k) (.getString x v)))))

(defn gety
  [ith x]
  (do (println " kkk gety " (.size x))
  (.getString x ith)))

(defn classpeek
  [x]
  (do
  (try
    (println "kkk class " (class (s/first x)))
    (catch Exception e))
    x))

(t/deftest join-two-tables
  ;data in 2 tables, see before-simple-seg2
  ;join from one to other and return values from the joinedon table
  (s/with-context
    sc testconf
    (let [ cst (fn []
                 (let [ st (Stest.)
                        res (.join st (.sc sc) "testks" "seg" "seg2" "id")]
                   (->> res
                        ;k contains source table, v contains joinedon table,
                        ;both as a list of CassandraRow objects
                        (s/map s-de/value)
                        (s/map (partial getx 0 1))
                        (s/key-by first)
                        (s/map-values rest)
                        ;(s/map (partial gety 1))
                        s/collect-map)))]
      (t/is (= {"a1" ["s2"] "a2" ["s3"]}
               (cassba {:before before-simple-seg2 :after after-seg} cst))))))

(t/deftest join-with-list-of-keys
  (s/with-context
    sc testconf
    (let [ cst (fn []
                 (let [ st (Stest.)
                        res (.joincol st (.sc sc)
                                      (doto (java.util.ArrayList.)
                                        (.add "a1") (.add "a2"))
                                      "testks" "seg" "id")]
                   (->> res
                        ;k contains source table, v contains joinedon table,
                        ;both as a list of CassandraRow objects
                        (s/map s-de/value)
                        (s/map (partial getx 0 1))
                        (s/key-by first)
                        (s/map-values rest)
                        s/collect-map)))]
      (t/is (= {"a1" '("s1") "a2" '("s1")}
               (cassba {:before before-simple-seg :after after-seg} cst))))))


(comment
  ;test fails
(t/deftest join-returns-map-withset-vals
  ;finally a join that returns a map, where key is given key, and
  ;value is a set
  (s/with-context
    sc testconf
    (let [ cst (fn []
                 (let [ st (Stest.)
                        res (.joincol2 st (.sc sc)
                                       (doto (java.util.ArrayList.)
                                         (.add "a1") (.add "a2"))
                                       "testks" "seg" "id")]
                   (zipmap (map s-de/key res) (map s-de/value res))))]
      (t/is (= {"a1" #{"s1" "s2"}, "a2" #{"s2" "s3"}}
               (cassba {:before before-seg :after after-seg} cst))))))
)

(t/with-test
  (defn joinon
    "given a sequence of keys (keyseq), this returns a map
    by joining on the given cassandra (keyspace,tablename), for the joincol
    column"
    [{:keys [sc keyseq keyspace tablename joincol]}]
    (fn []
      (let [ st (Stest.)
             al (java.util.ArrayList.)
             _ (doseq [i keyseq] (.add al i))

             res (.joincol2 st (.sc sc) al keyspace tablename joincol)]
        (zipmap (map s-de/key res) (map s-de/value res)))))

  (s/with-context
    sc testconf
    (let [ imap {:keyspace "testks" :tablename "seg" :joincol "id"
                 :sc sc :keyseq ["a1" "a2"]}]
      (t/is (= {"a1" #{"s1" "s2"}, "a2" #{"s2" "s3"}}
               (cassba {:before before-seg :after after-seg} (joinon imap)))))))

(defn make-map
  [x]
  (let [ks (map s-de/key x)
        ;create a Clojure set because set returned by Scala is a wrapper
        vs (map (comp set s-de/value) x)
    res (zipmap ks vs)]
    (println "class res " (class res) " " (class (first vs)))
    res))

(t/with-test
  (defn joinonrdd
    "given a sequence of keys (keyseq), this returns a map
    by joining on the given cassandra (keyspace,tablename), for the joincol
    column"
    [{:keys [keyseq keyspace tablename joincol]}]
    (let [ st (Stest.)
           rescnt (s/count keyseq)]
      (log/info "kkk joinonrdd for count "   rescnt )
      (if (> rescnt 0)
        (let [  res (.joincolrdd st keyseq keyspace tablename joincol)
                m1 (make-map res)
                res2 (s/collect-map keyseq)]
          (println "kkk cm " res2 " " m1)
          (merge-with into m1 res2))
      (do (println "kkk empty map, no join " ) {}))))
  (let [expected {"a2" #{"s3" "s1" "s2"}, "a3" #{"s1" "s3"}}
        expected2 {"a1" #{"s3" "s4" "s1" "s2"}}
        arg1 [(map #(str "a" %) (range 2 4)) (repeat 3 #{"s1" "s3"})]
        arg2 [["a1"] [#{"s3" "s4"}]]
        testfn (fn [[ks vs] ]
                 (s/with-context
                   sc testconf
                   (let [imap {:keyspace "testks" :tablename "seg" :joincol "id"
                               :keyseq (.rdd (s/into-pair-rdd sc (map s-de/tuple ks vs)))}]
                     (cassba {:before before-seg :after after-seg} (partial joinonrdd imap)))))
        [res1 res2] (map testfn [arg1 arg2])]
    (t/is (= res1 expected))
    (t/is (= res2 expected2))))
