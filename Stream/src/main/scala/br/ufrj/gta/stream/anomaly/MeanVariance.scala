package br.ufrj.gta.stream.anomaly

import util.control.Breaks._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.broadcast.Broadcast

object MeanVariance {
    private def summarize(df: DataFrame, featuresCol: String): Array[Vector] = {
        val row = df.select(
            Summarizer.metrics("mean", "variance")
                .summary(df(featuresCol))
                .as("summary"))
            .select("summary.mean", "summary.variance")
            .first()

        Array(row.getAs[Vector](0), row.getAs[Vector](1))
    }

    def model(df: DataFrame, threshold: Double, featuresCol: String = "features"): Array[Array[Double]] = {
        val summary = MeanVariance.summarize(df, featuresCol)

        val mean = summary(0).toDense.toArray
        val variance = summary(1).toDense.toArray

        val modelBottom = mean.zip(variance).map {
            case(x,y) => x - y * threshold
        }

        val modelTop = mean.zip(variance).map {
            case(x,y) => x + y * threshold
        }

        Array(modelBottom, modelTop)
    }

    def test(model: Broadcast[Array[Array[Double]]], df: DataFrame, featuresRange: Range): DataFrame = {
        val detect = udf {
            (f: Vector) => {
                var i = 0
                var label = "0"

                breakable {
                    for (i <- featuresRange) {
                        if (f(i - 1) < model.value(0)(i - 1) || f(i - 1) > model.value(1)(i - 1)) {
                            label = "1"
                            break
                        }
                    }
                }

                label
            }
        }

        df.withColumn("result", detect(df("features")))
    }

    def getAnomalies(model: Broadcast[Array[Array[Double]]], df: DataFrame, featuresRange: Range): DataFrame = {
        def filterAnomaly(r: Row): Boolean = {
            var i = 0
            val f: Vector = r.getAs[Vector](r.size - 1)

            for (i <- featuresRange) {
                if (f(i - 1) < model.value(0)(i - 1) || f(i - 1) > model.value(1)(i - 1)) {
                    return true
                }
            }

            false
        }

        df.filter(filterAnomaly _)
    }

    def getLegitimates(model: Broadcast[Array[Array[Double]]], df: DataFrame, featuresRange: Range): DataFrame = {
        def filterLegitimates(r: Row): Boolean = {
            var i = 0
            val f: Vector = r.getAs[Vector](r.size - 1)

            for (i <- featuresRange) {
                if (f(i - 1) < model.value(0)(i - 1) || f(i - 1) > model.value(1)(i - 1)) {
                    return false
                }
            }

            true
        }

        df.filter(filterLegitimates _)
    }
}
