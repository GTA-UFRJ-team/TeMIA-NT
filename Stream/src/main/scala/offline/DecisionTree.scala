package offline

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.PCA
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import br.ufrj.gta.stream.schema.GTA
import br.ufrj.gta.stream.util.{File, Metrics}

object DecisionTree {
    def main(args: Array[String]) {
        val sep = ","
        val labelCol = "label"

        val pcaFeaturesCol = "pcaFeatures"
        var featuresCol = "features"

        val schema = GTA.getSchema

        val spark = SparkSession.builder.appName("Stream").getOrCreate()

        if (args.length < 4) {
            println("Missing parameters")
            sys.exit(1)
        }

        val inputFile = args(0)
        val outputMetricsPath = File.appendSlash(args(1))
        val impurity = args(2)
        val maxDepth = args(3).toInt
        val pcaK: Option[Int] = try {
            Some(args(4).toInt)
        } catch {
            case e: Exception => None
        }

        val inputData = spark.read
            .option("sep", sep)
            .option("header", false)
            .schema(schema)
            .csv(inputFile)

        val featurizedData = GTA.featurize(inputData, featuresCol)
        val splitData = featurizedData.randomSplit(Array(0.7, 0.3))

        val (trainingData, testData, metricsFilename) = pcaK match {
            case Some(pcaK) => {
                val pca = new PCA()
                    .setInputCol(featuresCol)
                    .setOutputCol(pcaFeaturesCol)
                    .setK(pcaK)
                    .fit(splitData(0))

                featuresCol = pcaFeaturesCol

                (pca.transform(splitData(0)), pca.transform(splitData(1)), "offline_decision_tree_pca.csv")
            }
            case None => (splitData(0), splitData(1), "offline_decision_tree.csv")
        }

        val classifier = new DecisionTreeClassifier()
            .setFeaturesCol(featuresCol)
            .setLabelCol(labelCol)
            .setMaxDepth(maxDepth)
            .setImpurity(impurity)

        val model = classifier.fit(trainingData)

        val prediction = model.transform(testData)

        val predictionCol = classifier.getPredictionCol

        prediction.cache()

        Metrics.exportPrediction(
            Metrics.getPrediction(prediction, labelCol, predictionCol),
            outputMetricsPath + metricsFilename,
            "csv"
        )

        prediction.unpersist()

        spark.stop()
    }
}
