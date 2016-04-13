(ns kafcas.tes
  (:require [sparkling.conf :as conf]
            [sparkling.core :as s]
            [sparkling.serialization]
            ;[kafcas.core :as kc]
;            [sparkling.destructuring :as s-de]
;            [clojure.java.io :as io]
;            [qbits.alia :as alia]
            )
;  (:import [org.apache.spark.api.java JavaRDD JavaPairRDD JavaSparkContext]
;           [java.util HashMap Collections]
;           [java.io File]
;           [com.datastax.spark.connector.japi CassandraJavaUtil]
;           [org.cassandraunit.utils EmbeddedCassandraServerHelper]
;           [org.apache.spark.streaming.api.java JavaStreamingContext]
;           )
 (:gen-class)
  )

(comment
(s/with-context
    sc (-> (conf/spark-conf) (conf/master "local[*]") (conf/app-name "abc"))
    (->> (s/into-rdd sc (range 1 10))
         (s/map inc)
         (s/collect)))
  )
