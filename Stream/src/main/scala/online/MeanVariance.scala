package online

import org.apache.spark.ml.Model
import org.apache.spark.ml.feature.PCA
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

import br.ufrj.gta.stream.classification.anomaly.MeanVarianceClassifier
import br.ufrj.gta.stream.tuning.MeanVarianceCrossValidator
import br.ufrj.gta.stream.schema.GTA
import br.ufrj.gta.stream.util.{File, Metrics}

object MeanVariance {
    def main(args: Array[String]) {
        val sep = ","
        val maxFilesPerTrigger = 1
        val labelCol = "label"

        val pcaFeaturesCol = "pcaFeatures"
        var featuresCol = "features"

        val schema = GTA.getSchema

        val spark = SparkSession.builder.appName("Stream").getOrCreate()

        if (args.length < 6) {
            println("Missing parameters")
            sys.exit(1)
        }

        val inputTrainingFile = args(0)
        val inputTestPath = args(1)
        val outputPath = File.appendSlash(args(2))
        val outputMetricsPath = File.appendSlash(args(3))
        val timeoutStream = args(4).toLong
        val threshold = args(5).toDouble
        val pcaK: Option[Int] = try {
            Some(args(6).toInt)
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

        val (trainingData, testData) = pcaK match {
            case Some(pcaK) => {
                val featurizedTrainingData = GTA.featurize(inputTrainingData, featuresCol)
                val featurizedTestData = GTA.featurize(inputTestDataStream, featuresCol)

                val pca = new PCA()
                    .setInputCol(featuresCol)
                    .setOutputCol(pcaFeaturesCol)
                    .setK(pcaK)

                featuresCol = pcaFeaturesCol

                (pca.fit(featurizedTrainingData).transform(featurizedTrainingData), pca.fit(featurizedTrainingData).transform(featurizedTestData))
            }
            case None => (GTA.featurize(inputTrainingData, featuresCol), GTA.featurize(inputTestDataStream, featuresCol))
        }

        val mv = new MeanVarianceClassifier()
            .setFeaturesCol(featuresCol)
            .setLabelCol(labelCol)
            .setThreshold(threshold)

        val model = mv.fit(trainingData)

        val result = model.transform(testData)

        val predictionCol = mv.getPredictionCol

        val outputDataStream = result.select(result(labelCol), result(predictionCol)).writeStream
            .outputMode("append")
            .option("checkpointLocation", outputPath + "checkpoints/")
            .format("csv")
            .option("path", outputPath)
            .start()

        outputDataStream.awaitTermination(timeoutStream)

        val inputResultData = spark.read
            .option("sep", sep)
            .option("header", false)
            .schema(new StructType().add(labelCol, "integer").add(predictionCol, "double"))
            .csv(outputPath + "*.csv")

        val metricsFilename = "online_mean_variance.csv"

        Metrics.exportPredictionMetrics(
            Metrics.getPredictionMetrics(inputResultData, labelCol, predictionCol),
            outputMetricsPath + metricsFilename,
            "csv"
        )

        spark.stop()
    }
}
