package offline

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.PCA
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import br.ufrj.gta.stream.schema.GTA
import br.ufrj.gta.stream.simulation.Metrics
import br.ufrj.gta.stream.util.File

object DecisionTree {
    def main(args: Array[String]) {
        val sep = ","
        val labelCol = "label"

        val pcaFeaturesCol = "pcaFeatures"
        val defaultFeaturesCol = "features"
        var featuresCol = defaultFeaturesCol

        val schema = GTA.getSchema

        val spark = SparkSession.builder.appName("Stream").getOrCreate()

        if (args.length < 6) {
            println("Missing parameters")
            sys.exit(1)
        }

        val inputFile = args(0)
        val outputMetricsPath = File.appendSlash(args(1))
        val numSims = args(2).toInt
        val numCores = args(3).toInt
        val impurity = args(4)
        val maxDepth = args(5).toInt
        val pcaK: Option[Int] = try {
            Some(args(6).toInt)
        } catch {
            case e: Exception => None
        }

        val inputData = spark.read
            .option("sep", sep)
            .option("header", false)
            .schema(schema)
            .csv(inputFile)

        val featurizedData = GTA.featurize(inputData, featuresCol)

        var metricsFilename = "offline_decision_tree.csv"
        var metrics = Metrics.empty((Metrics.DefaultMetrics ++ List("Number of cores", "Training time", "Test time")): _*)

        for (i <- 0 until numSims) {
            val splitData = featurizedData.randomSplit(Array(0.7, 0.3))

            var startTime = System.currentTimeMillis()

            val (trainingData, testData) = pcaK match {
                case Some(pcaK) => {
                    val pca = new PCA()
                        .setInputCol(defaultFeaturesCol)
                        .setOutputCol(pcaFeaturesCol)
                        .setK(pcaK)
                        .fit(splitData(0))

                    featuresCol = pcaFeaturesCol

                    metricsFilename = "offline_decision_tree_pca.csv"

                    (pca.transform(splitData(0)), pca.transform(splitData(1)))
                }
                case None => (splitData(0), splitData(1))
            }

            val classifier = new DecisionTreeClassifier()
                .setFeaturesCol(featuresCol)
                .setLabelCol(labelCol)
                .setMaxDepth(maxDepth)
                .setImpurity(impurity)

            val model = classifier.fit(trainingData)

            val trainingTime = (System.currentTimeMillis() - startTime) / 1000.0

            startTime = System.currentTimeMillis()

            val prediction = model.transform(testData)

            val predictionCol = classifier.getPredictionCol

            prediction.cache()

            // Perform an action to accurately measure the test time
            prediction.count()

            val testTime = (System.currentTimeMillis() - startTime) / 1000.0

            metrics = metrics.add(Metrics.get(prediction, labelCol, predictionCol) + ("Number of cores" -> numCores, "Training time" -> trainingTime, "Test time" -> testTime))

            prediction.unpersist()
        }

        metrics.export(outputMetricsPath + metricsFilename, Metrics.FormatCsv)

        spark.stop()
    }
}
