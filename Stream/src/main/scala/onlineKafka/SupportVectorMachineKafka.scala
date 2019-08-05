package online

import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.SparkContext

import br.ufrj.gta.stream.schema.GTA
import br.ufrj.gta.stream.util.{File, Metrics}

object SupportVectorMachineKafka {
    def main(args: Array[String]) {
        val sep = ","
        val labelCol = "label"

        val pcaFeaturesCol = "pcaFeatures"
        var featuresCol = "features"

        val schema = GTA.getSchema

        val spark = SparkSession.builder.appName("Stream").config("spark.streaming.receiver.maxRate", 100).getOrCreate()
	import spark.implicits._

        if (args.length < 4) {
            println("Missing parameters")
            sys.exit(1)
        }

        val inputTrainingFile = args(0)
	val outputMetricsPath = File.appendSlash(args(1))
        val regParam = args(2).toDouble
        val maxIter = args(3).toInt
        val pcaK: Option[Int] = try {
            Some(args(4).toInt)
        } catch {
            case e: Exception => None
        }

        val inputTrainingData = spark.read
            .option("sep", sep)
            .option("header", false)
            .schema(schema)
            .csv(inputTrainingFile)

        val inputTestDataStream = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "testing")
	    .load()

	/* A linha abaixo pega lê o que foi publicado no kafka como string*/
	val query = inputTestDataStream.selectExpr("CAST(value AS STRING)").as[(String)]
	
        val featurizedTrainingData = GTA.featurize(inputTrainingData, featuresCol)
	
	val teste = query.withColumn("temp", split(col("value"),","))
	      .select((5 until 46).map(i => col("temp").getItem(i).cast("integer").as(s"col$i")): _*)
	
	val assembler = new VectorAssembler()
	      .setInputCols(Array("col5","col6","col7","col8","col9","col10","col11","col12","col13","col14","col15","col16","col17","col18","col19","col20","col21","col22","col23","col24","col25","col26","col27","col28","col29","col30","col31","col32","col33","col34","col35","col36","col37","col38","col39","col40","col41","col42","col43","col44")) 
	      .setOutputCol(featuresCol)

	val featurizedTestData = assembler.transform(teste)

        val (trainingData, testData, metricsFilename) = pcaK match {
            case Some(pcaK) => {
                val pca = new PCA()
                    .setInputCol(featuresCol)
                    .setOutputCol(pcaFeaturesCol)
                    .setK(pcaK)
                    .fit(featurizedTrainingData)

                featuresCol = pcaFeaturesCol

                (pca.transform(featurizedTrainingData), pca.transform(featurizedTestData), "online_support_vector_machine_pca.csv")
            }
            case None => (featurizedTrainingData, featurizedTestData, "online_support_vector_machine.csv")
        }

        val classifier = new LinearSVC()
            .setFeaturesCol(featuresCol)
            .setLabelCol(labelCol)
            .setRegParam(regParam)
            .setMaxIter(maxIter)

        val model = classifier.fit(trainingData)

        val prediction = model.transform(testData)

        def current_time = udf(() => {
            java.time.LocalDateTime.now().toString
        })

        val predictionWithTime = prediction.withColumn("time", current_time())

        val outputDataStream = predictionWithTime.select(to_json(struct($"prediction", $"col45", $"time")).alias("value")).writeStream
            .outputMode("append")
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("topic", "receive-test-1")
            .option("checkpointLocation", outputMetricsPath)
            .trigger(Trigger.Continuous(100))
            .start()

        outputDataStream.awaitTermination()

        spark.stop()
    }
}
