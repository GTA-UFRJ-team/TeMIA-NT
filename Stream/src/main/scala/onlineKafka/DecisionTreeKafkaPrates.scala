package online

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.PCA
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import org.apache.spark.sql.{Encoder, Encoders}

object DecisionTreeKafkaPrates {
    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("Stream").getOrCreate()

	import spark.implicits._

        val inputTestDataStream = spark.readStream.format("kafka")
		.option("kafka.bootstrap.servers", "localhost:9092")
		.option("subscribe", "testing")
		.load()

	var query = inputTestDataStream.selectExpr("CAST(value AS STRING)").as[(String)]
		.writeStream.format("console").start()

	print("Starting...")
	query.awaitTermination(20)
	
	Thread.sleep(5000)
	query.stop()

        spark.stop()
    }
}
