package online

import org.apache.spark.ml.Model
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

import br.ufrj.gta.stream.classification.anomaly.MeanVarianceClassifier
import br.ufrj.gta.stream.tuning.MeanVarianceCrossValidator
import br.ufrj.gta.stream.schema.GTA
import br.ufrj.gta.stream.util.{File, Metrics}

object MeanVariance {
    def main(args: Array[String]) {
        val sep = ","
        val maxFilesPerTrigger = 1
        val featuresCol = "features"
        val labelCol = "label"

        val schema = GTA.getSchema

        val spark = SparkSession.builder.appName("Stream").getOrCreate()

        if (args.length < 6) {
            println("Missing parameters")
            sys.exit(1)
        }

        val inputTrainingFile = args(0)
        val inputTestFile = args(1)
        val inputTestPath = args(2)
        val outputPath = File.appendSlash(args(3))
        val timeoutStream = args(4).toLong
        val threshold = args(5).toDouble

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

        val inputTestDataStream = spark.readStream
            .option("sep", sep)
            .option("header", false)
            .schema(schema)
            .csv(inputTestPath)

        val trainingData = GTA.featurize(inputTrainingData, featuresCol)
        val testData = GTA.featurize(inputTestData, featuresCol)

        val mv = new MeanVarianceClassifier()
            .setFeaturesCol(featuresCol)
            .setLabelCol(labelCol)
            .setThreshold(threshold)

        val model = mv.fit(trainingData)

        val result = model.transform(GTA.featurize(inputTestDataStream))

        val predictionCol = mv.getPredictionCol

        val outputDataStream = result.select(result(labelCol), result(predictionCol)).writeStream
            .outputMode("append")
            .option("checkpointLocation", outputPath + "checkpoints/")
            .format("csv")
            .option("path", outputPath + "result/")
            .start()

        outputDataStream.awaitTermination(timeoutStream)

        val inputResultData = spark.read
            .option("sep", sep)
            .option("header", false)
            .schema(new StructType().add(labelCol, "integer").add(predictionCol, "double"))
            .csv(outputPath + "result/*.csv")

        val metricsFilename = "online_mean_variance.csv"

        Metrics.exportPredictionMetrics(
            Metrics.getPredictionMetrics(inputResultData, predictionCol),
            outputPath + "metrics/" + metricsFilename,
            "csv"
        )

        spark.stop()
    }
}
