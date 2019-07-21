package offline

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.ml.classification.{LogisticRegression => LogisticRegressionClassifier}
import org.apache.spark.ml.feature.PCA
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

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

        if (args.length < 7) {
            println("Missing parameters")
            sys.exit(1)
        }

        val inputFile = args(0)
        val outputMetricsPath = File.appendSlash(args(1))
        val regParam = args(2).toDouble
	    val elasticNetParam = args(3).toDouble
	    val maxIter = args(4).toInt
        val numSims = args(5).toInt
        val numCores = args(6).toInt
        val pcaK: Option[Int] = try {
            Some(args(7).toInt)
        } catch {
            case e: Exception => None
        }

        val inputData = spark.read
            .option("sep", sep)
            .option("header", false)
            .schema(schema)
            .csv(inputFile)

        val featurizedData = GTA.featurize(inputData, featuresCol)

        var metricsFilename = "offline_logistic_regression.csv"
        var header: Iterable[_] = new ArrayBuffer()

        var ns = 0
        val metrics = new ArrayBuffer[Iterable[_]]()

        while (ns < numSims) {
            val splitData = featurizedData.randomSplit(Array(0.7, 0.3))

            var startTime = System.currentTimeMillis()

            val (trainingData, testData) = pcaK match {
                case Some(pcaK) => {
                    val pca = new PCA()
                        .setInputCol(featuresCol)
                        .setOutputCol(pcaFeaturesCol)
                        .setK(pcaK)
                        .fit(splitData(0))

                    featuresCol = pcaFeaturesCol

                    metricsFilename = "offline_logistic_regression_pca.csv"

                    (pca.transform(splitData(0)), pca.transform(splitData(1)))
                }
                case None => (splitData(0), splitData(1))
            }

            val classifier = new LogisticRegressionClassifier()
                .setFeaturesCol(featuresCol)
                .setLabelCol(labelCol)
                .setRegParam(regParam)
                .setElasticNetParam(elasticNetParam)
                .setMaxIter(maxIter)

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
