package eyeota

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import org.apache.spark.api.java._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.util._
import collection.JavaConversions._
import collection.JavaConverters._
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector.types._


class Stest {

  def join (sc:SparkContext, ks:String, tablesrc:String, tableon:String, joincol:String) :
  JavaRDD[Tuple2[CassandraRow,CassandraRow]]= {
    val internalJoin = sc.cassandraTable(ks,tablesrc).joinWithCassandraTable(ks,tableon).on(SomeColumns(joincol))
    return internalJoin.toJavaRDD()
  }

  def joincol (sc:SparkContext, keylst:List[String], ks:String, tableon:String, joincol:String) :
  JavaRDD[Tuple2[Tuple1[String],CassandraRow]]= {
    val keys:Seq[String]=keylst
    val source = sc.parallelize(keys).map(Tuple1(_))

    val internalJoin = source.joinWithCassandraTable(ks,tableon).on(SomeColumns(joincol))
    return internalJoin.toJavaRDD()
  }

  def joincol2 (sc:SparkContext, keylst:List[String], ks:String, tableon:String, joincol:String) :
  List[Tuple2[String,Set[String]]]= {
    val keys:Seq[String]=keylst
    val source = sc.parallelize(keys).map(Tuple1(_))

    println(" kkkjoincolrdd "+source.first().getClass.getName)
    val ij = source.joinWithCassandraTable(ks,tableon).on(SomeColumns(joincol))
    val internalJoin = ij.collect.map(_._2).map((x:CassandraRow)  => ((x.getString(0), x.getSet[String](1).asJava)))
    //println(" kkk class of internal "+internalJoin.getClass.getName)
    val res:List[Tuple2[String,Set[String]]]=internalJoin.toList
    return res
  }

  def getRowAsTuple (x:CassandraRow): Tuple2[String,Set[String]] ={
     return (x.getString(0), x.getSet[String](1).asJava)
  }

  def joincolrdd (keylst:RDD[Tuple2[String,Set[String]]],
                  ks:String, tableon:String, joincol:String) :
  List[Tuple2[String,Set[String]]]= {
    println("kkk joincolscala "+keylst.first()+" ks " +ks+" tableon "+tableon+" joincol " +joincol)
    val ij = keylst.joinWithCassandraTable(ks,tableon).on(SomeColumns(joincol))
    val internalJoin = ij.map(_._2).collect.map(getRowAsTuple)
    val res2:List[Tuple2[String,Set[String]]]=internalJoin.toList
    return res2
  }

}
