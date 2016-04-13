package eyeota;

import com.datastax.spark.connector.japi.*;
import com.datastax.spark.connector.japi.rdd.*;
import org.apache.spark.api.java.*;

public class Jtest {

  public static CassandraJavaPairRDD<DevProfile,DevProfile> join (JavaSparkContext sc){
     SparkContextJavaFunctions scjf = CassandraJavaUtil.javaFunctions(sc);
     RDDJavaFunctions rdf= new RDDJavaFunctions(scjf.cassandraTable("testks","seg2").rdd());

     return rdf.joinWithCassandraTable("testks","seg",
                                CassandraJavaUtil.allColumns,
                                CassandraJavaUtil.someColumns("id"),
                                CassandraJavaUtil.mapRowTo(DevProfile.class,java.util.Collections.<String,String>emptyMap()),
                                CassandraJavaUtil.mapToRow(DevProfile.class,java.util.Collections.<String,String>emptyMap()));


  }
}
