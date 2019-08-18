package br.ufrj.gta.stream.simulation

import java.lang.Exception

import org.apache.spark.sql.Dataset

import br.ufrj.gta.stream.util.File

private[simulation] class Metrics {
    var metrics = Map.empty[String, Seq[Any]]

    def this(names: String*) = {
        this()

        for (n <- names) {
            this.metrics = this.metrics + (n -> this.emptyEntry)
        }
    }

    private def emptyEntry: Seq[Any] = {
        Vector.empty[Any]
    }

    def add(values: Map[String, Any]): Metrics = {
        for ((k, v) <- values) {
            if (this.metrics.contains(k)) this.metrics = this.metrics + (k -> (this.metrics(k) :+ v))
        }

        this
    }

    def get(metric: String): Seq[Any] = this.metrics(metric)

    def getAll: Map[String, Seq[Any]] = this.metrics

    def export(filename: String, format: String): Unit = {
        val f = format.toLowerCase

        if (f == Metrics.FormatCsv) {
            File.exportCSV(filename, this.metrics.keys, this.metrics.values.transpose)
        } else {
            throw new Exception("Unsupported file format")
        }
    }
}

object Metrics {
    val FormatCsv = "csv"

    val DefaultMetrics = List("Test cases",
        "Real negative",
        "Real positive",
        "Predicted negative",
        "Predicted positive",
        "Precision",
        "Recall",
        "F1 Score",
        "Accuracy")

    def empty(metrics: String*): Metrics = {
        new Metrics(metrics: _*)
    }

    def get(predictions: Dataset[_], labelCol: String, predictionCol: String): Map[String, Any] = {
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
}
