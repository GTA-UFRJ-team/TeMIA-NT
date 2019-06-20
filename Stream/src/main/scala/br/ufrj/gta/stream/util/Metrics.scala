package br.ufrj.gta.stream.util

import java.lang.Exception

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.Dataset

object Metrics {
    def getPredictionMetrics(predictions: Dataset[_], predictionCol: String): Map[String, Any] = {
        val mce = new MulticlassClassificationEvaluator()

        Map(
            "# of test cases" -> predictions.count(),
            "# of Legitimates" -> predictions.where(predictions(predictionCol) === 0.0).count(),
            "# of Anomalies" -> predictions.where(predictions(predictionCol) === 1.0).count(),
            "Weighted Precision" -> mce.setMetricName("weightedPrecision").evaluate(predictions),
            "Weighted Recall" -> mce.setMetricName("weightedRecall").evaluate(predictions),
            "F1 Score" -> mce.setMetricName("f1").evaluate(predictions),
            "Accuracy" -> mce.setMetricName("accuracy").evaluate(predictions)
        )
    }

    def exportPredictionMetrics(predictions: Map[String, Any], filename: String, format: String): Unit = {
        val f = format.toLowerCase

        if (f == "csv") {
            File.exportCSV(filename, predictions.keys, Array(predictions.values))
        } else {
            throw new Exception("Unsupported file format")
        }
    }
}
