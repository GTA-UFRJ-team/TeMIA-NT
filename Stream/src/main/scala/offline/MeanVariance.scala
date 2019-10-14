package offline

import org.apache.spark.ml.feature.PCA
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import br.ufrj.gta.stream.metrics._
import br.ufrj.gta.stream.ml.classification.anomaly.MeanVarianceClassifier
import br.ufrj.gta.stream.schema.flow.Flowtbag
import br.ufrj.gta.stream.util.File

object MeanVariance {
    def main(args: Array[String]) {
        val sep = ","
        val labelCol = "label"

        val pcaFeaturesCol = "pcaFeatures"
        val defaultFeaturesCol = "features"
        var featuresCol = defaultFeaturesCol

        val schema = Flowtbag.getSchema

        val spark = SparkSession.builder.appName("Stream").getOrCreate()

        if (args.length < 6) {
            println("Missing parameters")
            sys.exit(1)
        }

        val inputTrainingFile = args(0)
        val inputTestFile = args(1)
        val metricsFilename = args(2)
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

        val featurizedTrainingData = Flowtbag.featurize(inputTrainingData, featuresCol)
        val featurizedTestData = Flowtbag.featurize(inputTestData, featuresCol)

        val metrics = new PredictionMetrics(PredictionMetrics.names ++ Array("Number of cores", "Training time", "Test time"))

        for (i <- 0 until numSims) {
            val splitData = Array(featurizedTrainingData.randomSplit(Array(0.7, 0.3))(0), featurizedTestData.randomSplit(Array(0.7, 0.3))(1))

            var startTime = System.currentTimeMillis()

            val (trainingData, testData) = pcaK match {
                case Some(pcaK) => {
                    val pca = new PCA()
                        .setInputCol(defaultFeaturesCol)
                        .setOutputCol(pcaFeaturesCol)
                        .setK(pcaK)
                        .fit(splitData(0))

                    featuresCol = pcaFeaturesCol

                    (pca.transform(splitData(0)), pca.transform(splitData(1)))
                }
                case None => (splitData(0), splitData(1))
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

            // Perform an action to accurately measure the test time
            prediction.count()

            val testTime = (System.currentTimeMillis() - startTime) / 1000.0

            metrics.add(metrics.getMetrics(prediction, labelCol, predictionCol) + ("Number of cores" -> numCores, "Training time" -> trainingTime, "Test time" -> testTime))

            prediction.unpersist()
        }

        metrics.export(metricsFilename, Metrics.FormatCsv)

        spark.stop()
    }
}
