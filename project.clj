(defproject kafcas "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [com.datastax.spark/spark-cassandra-connector-java_2.10 "1.6.0-M1"]
                 [com.datastax.spark/spark-cassandra-connector_2.10 "1.6.0-M1"]
                 [gorillalabs/sparkling "1.2.3"]
                 [embedded-kafka "0.3.0"]
                 [zookeeper-clj "0.9.1"]
                 [org.apache.zookeeper/zookeeper "3.4.8"]
                 [org.cassandraunit/cassandra-unit-shaded "2.1.9.2"]
                 ;due to cdnfe while loading buildTestMessenger
                 [org.apache.sshd/sshd-core "0.13.0"]
                 ;                 [cc.qbits/hayt "3.0.1"]
                 ;                 [im.chit/hara.reflect "2.2.17"]
                 [org.apache.ftpserver/ftpserver-core "1.0.6"]
                 [org.bouncycastle/bcpkix-jdk15on "1.54"]
                 [cc.qbits/alia-all "3.1.3"]
                 [cc.qbits/hayt "3.0.1"]
                 ;logging conf
                 [org.clojure/tools.logging "0.3.1"]
                 [ch.qos.logback/logback-classic "1.1.3"]
                 [clj-logging-config "1.9.12"]
                 ;kafka
                 [clj-kafka "0.3.5-SNAPSHOT"]
                 ;expectations used by clj-kafka
                 [expectations "1.4.45"]
                 ;reflection-to determine method types to call on Scala
                 [im.chit/vinyasa "0.4.3"]
                 ;                 [com.taoensso/timbre "4.3.1"]
                 ]
  :aot [ #".*"
         sparkling.serialization
         sparkling.destructuring
;         eyeota.msgutil
         kafcas.cass
         ; sparkling.conf
         ; kafcas.core
         ]
  :jvm-opts ^:replace ["-server" "-Xmx32g"]
  :profiles {;:default      [:dev :test]
             :test {:dependencies
                    [;[messaging/common "0.1"]
                     ;                     [messaging/wahoo "0.1"]
                     ;[org.apache.sshd/apache-sshd "1.1.0"]
                     ;[messaging/wahoo "0.1" :classifier "tests" :exclusions [joda-time]]
                     ]}
             :provided
             {:dependencies   [[org.apache.spark/spark-core_2.10 "1.6.0"
                                :exclusions [com.google.guava/guava] ]
                               [org.apache.spark/spark-core_2.10 "1.6.0" :classifier "tests"]
                               [org.apache.spark/spark-streaming_2.10 "1.6.0"]
                               [org.apache.spark/spark-streaming-kafka_2.10 "1.6.0"]
                               [org.apache.spark/spark-streaming_2.10 "1.6.0" :classifier "tests"]
                               [org.apache.spark/spark-mllib_2.10 "1.6.0"]
                               [org.scala-lang/scala-library "2.10.6"]
                               [org.scala-lang/scala-reflect "2.10.6"]
                               ]}
             :dev          {:plugins        [[lein-dotenv "RELEASE"]
                                             [lein-test-out "0.3.1"]
                                             [haruyama/lein-scalac "0.1.1"]]
                            ;if compile isn't the second task, then classdefnotfound
                            ;errors for sparking's registrator is seen.
                            ;hat tip to http://hypirion.com/musings/advanced-intermixing-java-clj
                            :prep-tasks      [["scalac"]
                                              ["javac"]
                                              ["compile"]]
                            :resource-paths ["data"]}}
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :test-paths ["test" "src/clojure"]
  ;else it doesn't find tests defined in the source folder
  :scala-source-paths ["src/scala"]
  :scala-version "2.10.6"
  )
