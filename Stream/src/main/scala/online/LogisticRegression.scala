package online

import org.apache.spark.ml.classification.{LogisticRegression => LogisticRegressionClassifier}
import org.apache.spark.ml.feature.PCA
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import br.ufrj.gta.stream.schema.GTA
import br.ufrj.gta.stream.util.{File, Metrics}

object LogisticRegression {
    def main(args: Array[String]) {
        val sep = ","
        val labelCol = "label"

        val pcaFeaturesCol = "pcaFeatures"
        var featuresCol = "features"

        val schema = GTA.getSchema

        val spark = SparkSession.builder.appName("Stream").getOrCreate()

        if (args.length < 8) {
            println("Missing parameters")
            sys.exit(1)
        }

        val inputTrainingFile = args(0)
        val inputTestPath = args(1)
        val outputPath = File.appendSlash(args(2))
        val outputMetricsPath = File.appendSlash(args(3))
        val timeoutStream = args(4).toLong
        val regParam = args(5).toDouble
	    val elasticNetParam = args(6).toDouble
        val maxIter = args(7).toInt
        val pcaK: Option[Int] = try {
            Some(args(8).toInt)
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
                    .fit(featurizedTrainingData)

                featuresCol = pcaFeaturesCol

                (pca.transform(featurizedTrainingData), pca.transform(featurizedTestData))
            }
            case None => (GTA.featurize(inputTrainingData, featuresCol), GTA.featurize(inputTestDataStream, featuresCol))
        }

        val lr = new LogisticRegressionClassifier()
            .setFeaturesCol(featuresCol)
            .setLabelCol(labelCol)
            .setRegParam(regParam)
            .setElasticNetParam(elasticNetParam)
            .setMaxIter(maxIter)

        val model = lr.fit(trainingData)

        val result = model.transform(testData)

        val predictionCol = lr.getPredictionCol

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

        val metricsFilename = "online_logistic_regression.csv"

        Metrics.exportPrediction(
            Metrics.getPrediction(inputResultData, labelCol, predictionCol),
            outputMetricsPath + metricsFilename,
            "csv"
        )

        spark.stop()
    }
}
