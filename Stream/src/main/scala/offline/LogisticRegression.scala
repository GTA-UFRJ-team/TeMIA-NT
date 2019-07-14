package offline

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

        if (args.length < 5) {
            println("Missing parameters")
            sys.exit(1)
        }

        val inputFile = args(0)
        val outputMetricsPath = File.appendSlash(args(1))
        val regParam = args(2).toDouble
	    val elasticNetParam = args(3).toDouble
	    val maxIter = args(4).toInt
        val pcaK: Option[Int] = try {
            Some(args(5).toInt)
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

        val lr = new LogisticRegressionClassifier()
            .setFeaturesCol(featuresCol)
            .setLabelCol(labelCol)
            .setRegParam(regParam)
            .setElasticNetParam(elasticNetParam)
            .setMaxIter(maxIter)

        val model = lr.fit(trainingData)

        val result = model.transform(testData)

        result.cache()

        val predictionCol = lr.getPredictionCol

        val metricsFilename = "offline_logistic_regression.csv"

        Metrics.exportPrediction(
            Metrics.getPrediction(result, labelCol, predictionCol),
            outputMetricsPath + metricsFilename,
            "csv"
        )

        result.unpersist()

        spark.stop()
    }
}
