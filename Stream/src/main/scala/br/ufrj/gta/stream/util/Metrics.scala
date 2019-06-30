package br.ufrj.gta.stream.util

import java.lang.Exception

import org.apache.spark.sql.Dataset

object Metrics {
    def getPrediction(predictions: Dataset[_], labelCol: String, predictionCol: String): Map[String, Any] = {
        val size = predictions.count()

        val rn = predictions.where(predictions(labelCol) === 0.0).count()
        val rp = predictions.where(predictions(labelCol) === 1.0).count()

        val pn = predictions.where(predictions(predictionCol) === 0.0).count()
        val pp = predictions.where(predictions(predictionCol) === 1.0).count()

        val tp = predictions.where(predictions(predictionCol) === 1.0 && predictions(labelCol) === 1.0).count()
        val tn = predictions.where(predictions(predictionCol) === 0.0 && predictions(labelCol) === 0.0).count()
        val fp = predictions.where(predictions(predictionCol) === 1.0 && predictions(labelCol) === 0.0).count()
        val fn = predictions.where(predictions(predictionCol) === 0.0 && predictions(labelCol) === 1.0).count()

        Map(
            "Test cases" -> size,
            "Real negative" -> rn,
            "Real positive" -> rp,
            "Predicted negative" -> pn,
            "Predicted positive" -> pp,
            "Precision" -> 1.0 * tp / (tp + fp),
            "Recall" -> 1.0 * tp / (tp + fn),
            "F1 Score" -> 2.0 * tp / (2 * tp + fp + fn),
            "Accuracy" -> 1.0 * (tp + tn) / (tp + tn + fp + fn)
        )
    }

    def exportPrediction(predictions: Map[String, Any], filename: String, format: String): Unit = {
        val f = format.toLowerCase

        if (f == "csv") {
            File.exportCSV(filename, predictions.keys, Array(predictions.values))
        } else {
            throw new Exception("Unsupported file format")
        }
    }
}
