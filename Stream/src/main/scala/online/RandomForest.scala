package online

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{PCA, VectorIndexer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import br.ufrj.gta.stream.metrics._
import br.ufrj.gta.stream.schema.flow.Flowtbag
import br.ufrj.gta.stream.util.File

object RandomForest {
    def main(args: Array[String]) {
        val sep = ","
        val labelCol = "label"

        val pcaFeaturesCol = "pcaFeatures"
        //val indexedFeaturesCol = "indexedFeatures"
        var featuresCol = "features"

        val schema = Flowtbag.getSchema

        val spark = SparkSession.builder.appName("Stream").getOrCreate()

        if (args.length < 9) {
            println("Missing parameters")
            sys.exit(1)
        }

        val inputTrainingFile = args(0)
        val timeoutStream = args(1).toLong
        val inputTestPath = args(2)
        val outputPath = File.appendSlash(args(3))
        val progressFilename = args(4)
        val metricsFilename = args(5)
        val numTrees = args(6).toInt
        val impurity = args(7)
        val maxDepth = args(8).toInt
        //val maxCategories = args(8).toInt
        val pcaK: Option[Int] = try {
            Some(args(9).toInt)
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

        val classifier = new RandomForestClassifier()
            .setFeaturesCol(featuresCol)
            .setLabelCol(labelCol)
            .setNumTrees(numTrees)
            .setImpurity(impurity)
            .setMaxDepth(maxDepth)

        // TODO: add a vector indexer to this pipeline
        val pipeline = new Pipeline()
            .setStages(Array(classifier))

        val model = pipeline.fit(trainingData)

        val prediction = model.transform(testData)

        val predictionCol = classifier.getPredictionCol

        val streamingMetrics = new StreamingMetrics(StreamingMetrics.names)

        spark.streams.addListener(streamingMetrics.getListener)

        val outputDataStream = prediction.select(prediction(labelCol), prediction(predictionCol)).writeStream
            .outputMode("append")
            .option("checkpointLocation", outputPath + "checkpoints/")
            .format("csv")
            .option("path", outputPath)
            .start()

        outputDataStream.awaitTermination(timeoutStream)

        val metrics = new PredictionMetrics(PredictionMetrics.names)

        val inputResultData = spark.read
            .option("sep", sep)
            .option("header", false)
            .schema(new StructType().add(labelCol, "integer").add(predictionCol, "double"))
            .csv(outputPath + "*.csv")

        metrics.add(metrics.getMetrics(inputResultData, labelCol, predictionCol))

        metrics.export(metricsFilename, Metrics.FormatCsv)

        streamingMetrics.export(progressFilename, Metrics.FormatCsv)

        spark.stop()
    }
}
