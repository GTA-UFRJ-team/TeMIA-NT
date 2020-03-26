package br.ufrj.gta.stream.metrics

import java.lang.Exception

import br.ufrj.gta.stream.util.File

abstract class Metrics(val names: Array[String]) {
    var data = Map.empty[String, Seq[Any]]

    for (n <- this.names) {
        this.data = this.data + (n -> this.emptyEntry)
    }

    private def emptyEntry: Seq[Any] = {
        Vector.empty[Any]
    }

    def add(values: Map[String, Any]): Metrics = {
        for ((k, v) <- values) {
            if (this.data.contains(k)) this.data = this.data + (k -> (this.data(k) :+ v))
        }

        this
    }

    def get(metric: String): Seq[Any] = this.data(metric)

    def getAll: Map[String, Seq[Any]] = this.data

    def export(filename: String, format: String): Unit = {
        val f = format.toLowerCase

        if (f == Metrics.FormatCsv) {
            File.exportCSV(filename, this.data.keys, this.data.values.transpose)
        } else {
            throw new Exception("Unsupported file format")
        }
    }
}

object Metrics {
    val FormatCsv = "csv"
}
