package offline

import org.apache.spark.ml.Model
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

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

        if (args.length < 4) {
            println("Missing parameters")
            sys.exit(1)
        }

        val inputTrainingFile = args(0)
        val inputTestFile = args(1)
        val outputMetricsPath = File.appendSlash(args(2))
        val threshold = args(3).toDouble

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

        val trainingData = GTA.featurize(inputTrainingData, featuresCol)
        val testData = GTA.featurize(inputTestData, featuresCol)

        val mv = new MeanVarianceClassifier()
            .setFeaturesCol(featuresCol)
            .setLabelCol(labelCol)
            .setThreshold(threshold)

        val model = mv.fit(trainingData)

        val result = model.transform(testData.randomSplit(Array(0.7, 0.3))(1))

        result.cache()

        val predictionCol = mv.getPredictionCol

        val metricsFilename = "offline_mean_variance.csv"

        Metrics.exportPredictionMetrics(
            Metrics.getPredictionMetrics(result, labelCol, predictionCol),
            outputMetricsPath + metricsFilename,
            "csv"
        )

        result.unpersist()

        spark.stop()
    }
}
