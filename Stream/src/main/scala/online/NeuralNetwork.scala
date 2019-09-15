package online

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.feature.PCA
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import br.ufrj.gta.stream.schema.flow.Flowtbag
import br.ufrj.gta.stream.util.{File, Metrics}

object NeuralNetwork {
    def main(args: Array[String]) {
        val sep = ","
        val labelCol = "label"

        val pcaFeaturesCol = "pcaFeatures"
        var featuresCol = "features"

        val schema = Flowtbag.getSchema

        val spark = SparkSession.builder.appName("Stream").getOrCreate()

        if (args.length < 7) {
            println("Missing parameters")
            sys.exit(1)
        }

        val inputTrainingFile = args(0)
        val inputTestPath = args(1)
        val outputPath = File.appendSlash(args(2))
        val metricsFilename = args(3)
        val timeoutStream = args(4).toLong
        val layers = args(5).split(',').map(l => l.toInt)
        val maxIter = args(6).toInt
        val pcaK: Option[Int] = try {
            Some(args(7).toInt)
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

        val featurizedTrainingData = Flowtbag.featurize(inputTrainingData, featuresCol)
        val featurizedTestData = Flowtbag.featurize(inputTestDataStream, featuresCol)

        val (trainingData, testData) = pcaK match {
            case Some(pcaK) => {
                val pca = new PCA()
                    .setInputCol(featuresCol)
                    .setOutputCol(pcaFeaturesCol)
                    .setK(pcaK)
                    .fit(featurizedTrainingData)

                featuresCol = pcaFeaturesCol

                (pca.transform(featurizedTrainingData), pca.transform(featurizedTestData))
            }
            case None => (featurizedTrainingData, featurizedTestData)
        }

        val classifier = new MultilayerPerceptronClassifier()
            .setFeaturesCol(featuresCol)
            .setLabelCol(labelCol)
            .setLayers(layers)
            .setMaxIter(maxIter)

        val model = classifier.fit(trainingData)

        val prediction = model.transform(testData)

        val predictionCol = classifier.getPredictionCol

        val outputDataStream = prediction.select(prediction(labelCol), prediction(predictionCol)).writeStream
            .outputMode("append")
            .option("checkpointLocation", outputPath + "checkpoints/")
            .format("csv")
            .option("path", outputPath)
            .start()

        outputDataStream.awaitTermination(timeoutStream)

        var metrics = Metrics.empty(Metrics.DefaultMetrics: _*)

        val inputResultData = spark.read
            .option("sep", sep)
            .option("header", false)
            .schema(new StructType().add(labelCol, "integer").add(predictionCol, "double"))
            .csv(outputPath + "*.csv")

        metrics = metrics.add(Metrics.getPrediction(inputResultData, labelCol, predictionCol))

        metrics.export(metricsFilename, Metrics.FormatCsv)

        spark.stop()
    }
}
