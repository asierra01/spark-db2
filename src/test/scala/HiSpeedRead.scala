/**
  * (C) Copyright IBM Corp. 2016
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *
  */
import com.ibm.spark.ibmdataserver.Constants
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.log4j.Logger

object HiSpeedRead {

  def main(args: Array[String]) {
    //val logger = Logger.getLogger(classOf[HiSpeedRead$]) 
    
    val DB2_CONNECTION_URL = "jdbc:db2://localhost:50000/sample:traceFile=./mytrace.log;"
    
    val conf = new SparkConf().
             setMaster("local[2]").
             setAppName("read test")
             //.setJars(Array("/Users/mac/sqllib/java/db2jcc.jar"))
  

    val sparkContext = new SparkContext(conf)

    val sqlContext = new SQLContext(sparkContext)

    val clazz = Class.forName("com.ibm.db2.jcc.DB2Driver")
    var another_clazz = Class.forName("HiSpeedRead")
    val logger = Logger.getLogger(another_clazz) 
    val username = scala.util.Properties.envOrElse("DB2_USER",     "mac" )
    val password = scala.util.Properties.envOrElse("DB2_PASSWORD", "mac" )

    val jdbcRdr = sqlContext.read.format("com.ibm.spark.ibmdataserver")
      .option("url", DB2_CONNECTION_URL)
      // .option(Constants.TABLE, tableName)
      .option("user", username)
      .option("password", password)
      //.option("dbtable", "employee")
      .option("dbtable", "customer") //this crash as customer 
                                     //has xml should be treated as SQL_BINARY field 
      .load()

    jdbcRdr.show()
    jdbcRdr.sparkSession.close()
    //sys.ShutdownHookThread 
         //{
            logger.info("Gracefully stopping Spark Streaming Application")
            sparkContext.stop()
            logger.info("Application stopped")
          //}
  }
}
