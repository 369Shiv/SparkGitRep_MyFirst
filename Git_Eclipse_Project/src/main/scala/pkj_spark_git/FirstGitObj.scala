package pkj_spark_git
import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.io.Source
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

object FirstGitObj {
  def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("Project").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("Error")

					val spark = SparkSession.builder().enableHiveSupport().config("hive.exec.dynamic.dynamic.partition.mode","nonstrict").getOrCreate()
					import spark.implicits._
					val hc = new HiveContext(sc)
			    import hc.implicits._

          //load("hdfs:///user/cloudera/sparkProjDataAVRO/projectsample.avro")
			    ///user/cloudera/sparkProjDataAVRO/projectsample.avro
					val avroDF = spark.read.format("com.databricks.spark.avro").load("hdfs:///user/cloudera/sparkProjDataAVRO/projectsample.avro")
					avroDF.show()
					avroDF.printSchema()

					println("***** Load URL data *****")
					val URLdata = Source.fromURL("https://randomuser.me/api/0.8/?results=10").mkString
					val URL_rdd = sc.parallelize(List(URLdata))
					val URL_df = spark.read.json(URL_rdd)
					URL_df.show()
					URL_df.printSchema()

					val flatURL = URL_df.withColumn("results", explode(col("results")))


					flatURL.show()
					flatURL.printSchema()

					val finalFlat = flatURL.select(
							"nationality",
							"results.user.cell",
							"results.user.dob",
							"results.user.email",
							"results.user.gender",
							"results.user.location.city",
						  "results.user.location.state",
							"results.user.location.street",
							"results.user.location.zip",
							"results.user.md5",
							"results.user.name.first",
							"results.user.name.last",
							"results.user.name.title",
							"results.user.password",
							"results.user.phone",
							"results.user.picture.large",
							"results.user.picture.medium",
							"results.user.picture.thumbnail",
							"results.user.registered",
							"results.user.salt",
							"results.user.sha1",
							"results.user.sha256",
							"results.user.username",
							"seed",
							"version"
							)
							finalFlat.show()
							finalFlat.printSchema()
							
							val rm_NumDF  = finalFlat.withColumn("username",regexp_replace(col("username"),"([0-9])",""))
							
              rm_NumDF.show()
              
              val BrdLeftJoin = avroDF.join(broadcast(rm_NumDF),Seq("username"),"left")
              BrdLeftJoin.show()
             
              println("****** Not Avilable Customer ******")
              val NotAvilableCust = BrdLeftJoin.filter(col("nationality").isNull)
              NotAvilableCust.show()
                           
              println("****** Avilable Customer ******")
              val AvilableCust = BrdLeftJoin.filter(col("nationality").isNotNull)
              AvilableCust.show()
              
              println("******  not available customers replace nulls in strings with  ******")
              val replaceNull = NotAvilableCust.na.fill("Not avilable").na.fill(0)
              replaceNull.show()
              
              //customerDBrevsion
              
              AvilableCust.write.format("parquet").saveAsTable("customerDBrevsion.avilable_customer")
              replaceNull.write.format("parquet").saveAsTable("customerDBrevsion.non_avilable_customer")
	}
  
}