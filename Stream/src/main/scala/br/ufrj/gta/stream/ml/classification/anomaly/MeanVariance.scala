package br.ufrj.gta.stream.ml.classification.anomaly

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.ml.{Predictor, PredictionModel}
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.functions._

import br.ufrj.gta.stream.ml.param._

case class FeaturesMeanVariance(mean: Array[Double], variance: Array[Double])

case class MeanVarianceLimits(lower: Array[Double], upper: Array[Double])

class MeanVarianceClassifier(override val uid: String)
    extends Predictor[Vector, MeanVarianceClassifier, MeanVarianceModel]
    with HasThresholdParams {

    def this() = this(Identifiable.randomUID("mvc"))

    private def getFeaturesMeanVariance(dataset: Dataset[_]): FeaturesMeanVariance = {
        val row = dataset.select(
            Summarizer.metrics("mean", "variance")
                .summary(dataset($(this.featuresCol)))
                .as("summary"))
            .select("summary.mean", "summary.variance")
            .first()

        FeaturesMeanVariance(row.getAs[Vector](0).toArray, row.getAs[Vector](1).toArray)
    }

    override def train(dataset: Dataset[_]): MeanVarianceModel = {
        val summary = this.getFeaturesMeanVariance(dataset)

        val mean = summary.mean
        val variance = summary.variance

        val lowerLimit = mean.zip(variance).map {
            case (x,y) => x - y * $(this.threshold)
        }

        val upperLimit = mean.zip(variance).map {
            case (x,y) => x + y * $(this.threshold)
        }

        new MeanVarianceModel(this.uid, MeanVarianceLimits(lowerLimit, upperLimit))
    }

    override def copy(extra: ParamMap): MeanVarianceClassifier = defaultCopy(extra)
}

private[ml] class MeanVarianceModel(
        override val uid: String,
        val limits: MeanVarianceLimits)
    extends PredictionModel[Vector, MeanVarianceModel] {

    require(this.limits.lower.size == this.limits.upper.size, s"The sizes of lower and upper limits arrays must be equal")

    override val numFeatures = this.limits.lower.size

    override def copy(extra: ParamMap): MeanVarianceModel = {
        copyValues(new MeanVarianceModel(this.uid, this.limits)).setParent(this.parent)
    }

    override def predict(features: Vector): Double = {
        var i = 0

        for (i <- 0 until this.numFeatures) {
            if (features(i) < this.limits.lower(i) || features(i) > this.limits.upper(i)) {
                return 1.0
            }
        }

        0.0
    }
}
