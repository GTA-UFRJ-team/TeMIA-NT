package online

import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.feature.PCA
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import br.ufrj.gta.stream.schema.GTA
import br.ufrj.gta.stream.util.{File, Metrics}

object SupportVectorMachine {
    def main(args: Array[String]) {
        val sep = ","
        val labelCol = "label"

        val pcaFeaturesCol = "pcaFeatures"
        var featuresCol = "features"

        val schema = GTA.getSchema

        val spark = SparkSession.builder.appName("Stream").getOrCreate()

        if (args.length < 7) {
            println("Missing parameters")
            sys.exit(1)
        }

        val inputTrainingFile = args(0)
        val inputTestPath = args(1)
        val outputPath = File.appendSlash(args(2))
        val outputMetricsPath = File.appendSlash(args(3))
        val timeoutStream = args(4).toLong
        val regParam = args(5).toDouble
        val maxIter = args(6).toInt
        val pcaK: Option[Int] = try {
            Some(args(7).toInt)
        } catch {
            case e: Exception => None
        }

        val inputTrainingData = spark.read
            .option("sep", sep)
            .option("header", false)
            .schema(schema)
            .csv(inputTrainingFile)

        val inputTestDataStream = spark.readStream
            .option("sep", sep)
            .option("header", false)
            .schema(schema)
            .csv(inputTestPath)

        val featurizedTrainingData = GTA.featurize(inputTrainingData, featuresCol)
        val featurizedTestData = GTA.featurize(inputTestDataStream, featuresCol)

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

        val predictionCol = classifier.getPredictionCol

        val outputDataStream = prediction.select(prediction(labelCol), prediction(predictionCol)).writeStream
            .outputMode("append")
            .option("checkpointLocation", outputPath + "checkpoints/")
            .format("csv")
            .option("path", outputPath)
            .start()

        outputDataStream.awaitTermination(timeoutStream)

        var metrics = Metrics.empty(Metrics.DefaultMetrics: _*)

        val inputResultData = spark.read
            .option("sep", sep)
            .option("header", false)
            .schema(new StructType().add(labelCol, "integer").add(predictionCol, "double"))
            .csv(outputPath + "*.csv")

        metrics = metrics.add(Metrics.getPrediction(inputResultData, labelCol, predictionCol))

        metrics.export(outputMetricsPath + metricsFilename, Metrics.FormatCsv)

        spark.stop()
    }
}
