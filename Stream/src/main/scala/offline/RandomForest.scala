package offline

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{PCA, VectorIndexer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import br.ufrj.gta.stream.schema.GTA
import br.ufrj.gta.stream.util.{File, Metrics}

object RandomForest {
    def main(args: Array[String]) {
        val sep = ","
        val labelCol = "label"

        val pcaFeaturesCol = "pcaFeatures"
        val indexedFeaturesCol = "indexedFeatures"
        var featuresCol = "features"

        val schema = GTA.getSchema

        val spark = SparkSession.builder.appName("Stream").getOrCreate()

        if (args.length < 6) {
            println("Missing parameters")
            sys.exit(1)
        }

        val inputFile = args(0)
        val outputMetricsPath = File.appendSlash(args(1))
        val numTrees = args(2).toInt
        val impurity = args(3)
        val maxDepth = args(4).toInt
        val maxCategories = args(5).toInt
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

        val fi = new VectorIndexer()
            .setInputCol(featuresCol)
            .setOutputCol(indexedFeaturesCol)
            .setMaxCategories(maxCategories)
            .fit(featurizedData)

        val splitData = featurizedData.randomSplit(Array(0.7, 0.3))

        val (trainingData, testData) = pcaK match {
            case Some(pcaK) => {
                val featurizedTrainingData = splitData(0)
                val featurizedTestData = splitData(1)

                val pca = new PCA()
                    .setInputCol(featuresCol)
                    .setOutputCol(pcaFeaturesCol)
                    .setK(pcaK)
                    .fit(featurizedData)

                featuresCol = pcaFeaturesCol

                (pca.transform(splitData(0)), pca.transform(splitData(1)))
            }
            case None => (splitData(0), splitData(1))
        }

        val rf = new RandomForestClassifier()
            .setFeaturesCol(featuresCol)
            .setLabelCol(labelCol)
            .setNumTrees(numTrees)
            .setImpurity(impurity)
            .setMaxDepth(maxDepth)

        val pl = new Pipeline()
            .setStages(Array(fi, rf))

        val model = pl.fit(trainingData)

        val result = model.transform(testData)

        result.cache()

        val predictionCol = rf.getPredictionCol

        val metricsFilename = "offline_random_forest.csv"

        Metrics.exportPrediction(
            Metrics.getPrediction(result, labelCol, predictionCol),
            outputMetricsPath + metricsFilename,
            "csv"
        )

        result.unpersist()

        spark.stop()
    }
}
