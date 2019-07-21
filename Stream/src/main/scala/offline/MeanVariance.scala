package offline

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.ml.feature.PCA
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import br.ufrj.gta.stream.classification.anomaly.MeanVarianceClassifier
import br.ufrj.gta.stream.schema.GTA
import br.ufrj.gta.stream.util.{File, Metrics}

object MeanVariance {
    def main(args: Array[String]) {
        val sep = ","
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
        val inputTestFile = args(1)
        val outputMetricsPath = File.appendSlash(args(2))
        val numSims = args(3).toInt
        val numCores = args(4).toInt
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

        val inputTestData = spark.read
            .option("sep", sep)
            .option("header", false)
            .schema(schema)
            .csv(inputTestFile)

        val featurizedTrainingData = GTA.featurize(inputTrainingData, featuresCol)

        var metricsFilename = "offline_mean_variance.csv"
        var header: Iterable[_] = new ArrayBuffer()

        var ns = 0
        val metrics = new ArrayBuffer[Iterable[_]]()

        while (ns < numSims) {
            val featurizedTestData = GTA.featurize(inputTestData, featuresCol).randomSplit(Array(0.7, 0.3))(1)

            var startTime = System.currentTimeMillis()

            val (trainingData, testData) = pcaK match {
                case Some(pcaK) => {
                    val pca = new PCA()
                        .setInputCol(featuresCol)
                        .setOutputCol(pcaFeaturesCol)
                        .setK(pcaK)
                        .fit(featurizedTrainingData)

                    featuresCol = pcaFeaturesCol

                    metricsFilename = "offline_mean_variance_pca.csv"

                    (pca.transform(featurizedTrainingData), pca.transform(featurizedTestData))
                }
                case None => (featurizedTrainingData, featurizedTestData)
            }

            val classifier = new MeanVarianceClassifier()
                .setFeaturesCol(featuresCol)
                .setLabelCol(labelCol)
                .setThreshold(threshold)

            val model = classifier.fit(trainingData)

            val trainingTime = (System.currentTimeMillis() - startTime) / 1000.0

            startTime = System.currentTimeMillis()

            val prediction = model.transform(testData)

            val predictionCol = classifier.getPredictionCol

            prediction.cache()

            val testTime = (System.currentTimeMillis() - startTime) / 1000.0

            val metricsTmp = Metrics.getPrediction(prediction, labelCol, predictionCol) + ("Number of cores" -> numCores, "Training time" -> trainingTime, "Test time" -> testTime)

            header = metricsTmp.keys

            metrics += metricsTmp.values

            prediction.unpersist()
            ns += 1
        }

        File.exportCSV(outputMetricsPath + metricsFilename, header, metrics)

        spark.stop()
    }
}
