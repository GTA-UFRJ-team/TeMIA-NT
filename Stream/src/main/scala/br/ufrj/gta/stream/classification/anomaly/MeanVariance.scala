package br.ufrj.gta.stream.classification.anomaly

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.{Predictor, PredictionModel}
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.functions._

case class FeaturesMeanVariance(mean: Vector, variance: Vector)

case class MeanVarianceLimits(lower: Vector, upper: Vector)

private[classification] trait MeanVarianceClassifierParams extends Params {
    var threshold = new DoubleParam(this, "threshold", "threshold parameter (>= 0)")

    def getThreshold: DoubleParam = this.threshold

    def setThreshold(value: Double): this.type = {
        require(value >= 0.0, s"Threshold value must not be negative")
        set(this.threshold, value)
    }
}

class MeanVarianceClassifier(override val uid: String)
    extends Predictor[Vector, MeanVarianceClassifier, MeanVarianceModel]
    with MeanVarianceClassifierParams {

    def this() = this(Identifiable.randomUID("mvc"))

    private def getFeaturesMeanVariance(dataset: Dataset[_]): FeaturesMeanVariance = {
        val row = dataset.select(
            Summarizer.metrics("mean", "variance")
                .summary(dataset($(this.featuresCol)))
                .as("summary"))
            .select("summary.mean", "summary.variance")
            .first()

        FeaturesMeanVariance(row.getAs[Vector](0), row.getAs[Vector](1))
    }

    override def train(dataset: Dataset[_]): MeanVarianceModel = {
        val summary = this.getFeaturesMeanVariance(dataset)

        val mean = summary.mean.toDense.toArray
        val variance = summary.variance.toDense.toArray

        val lowerLimit = mean.zip(variance).map {
            case(x,y) => x - y * $(this.threshold)
        }

        val upperLimit = mean.zip(variance).map {
            case(x,y) => x + y * $(this.threshold)
        }

        new MeanVarianceModel(this.uid, MeanVarianceLimits(Vectors.dense(lowerLimit), Vectors.dense(upperLimit)))
    }

    override def copy(extra: ParamMap): MeanVarianceClassifier = defaultCopy(extra)
}

private[stream] class MeanVarianceModel(
        override val uid: String,
        val limits: MeanVarianceLimits)
    extends PredictionModel[Vector, MeanVarianceModel] {

    require(this.limits.lower.size == this.limits.upper.size, s"The sizes of lower and upper limits Vectors must be the equal")

    override val numFeatures = this.limits.lower.size

    override def copy(extra: ParamMap): MeanVarianceModel = {
        copyValues(new MeanVarianceModel(this.uid, this.limits)).setParent(this.parent)
    }

    override def predict(features: Vector): Double = {
        var i = 0

        for (i <- 1 to this.numFeatures) {
            if (features(i - 1) < this.limits.lower(i - 1) || features(i - 1) > this.limits.upper(i - 1)) {
                return 1.0
            }
        }

        0.0
    }
}
