package offline

import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.feature.PCA
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

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

        if (args.length < 4) {
            println("Missing parameters")
            sys.exit(1)
        }

        val inputFile = args(0)
        val outputMetricsPath = File.appendSlash(args(1))
        val regParam = args(2).toDouble
	    val maxIter = args(3).toInt
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

        val sv = new LinearSVC()
            .setFeaturesCol(featuresCol)
            .setLabelCol(labelCol)
            .setRegParam(regParam)
            .setMaxIter(maxIter)

        val model = sv.fit(trainingData)

        val result = model.transform(testData)

        result.cache()

        val predictionCol = sv.getPredictionCol

        val metricsFilename = "offline_support_vector_machine.csv"

        Metrics.exportPrediction(
            Metrics.getPrediction(result, labelCol, predictionCol),
            outputMetricsPath + metricsFilename,
            "csv"
        )

        result.unpersist()

        spark.stop()
    }
}
