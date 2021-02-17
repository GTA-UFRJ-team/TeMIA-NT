package br.ufrj.gta.stream.tuning

import java.lang.UnsupportedOperationException;

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.util._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.StructType

import br.ufrj.gta.stream.classification.anomaly.MeanVarianceModel

class MeanVarianceCrossValidator (override val uid: String)
    extends CrossValidator {

    def this() = this(Identifiable.randomUID("mvcv"))

    private def kFold(trainingDataset: Dataset[_], validationDataset: Dataset[_], numFolds: Int, seed: Long): Array[(RDD[Row], RDD[Row])] = {
        MLUtils.kFold(trainingDataset.toDF.rdd, numFolds, seed).map(a => { a._1 }).zip(
            MLUtils.kFold(validationDataset.toDF.rdd, numFolds, seed).map(a => { a._2 })
        )
    }

    override def fit(dataset: Dataset[_]) = {
        throw new UnsupportedOperationException("You must call fit(trainingDataset: Dataset[_], validationDataset: Dataset[_]) instead");
    }

    def fit(trainingDataset: Dataset[_], validationDataset: Dataset[_]): MeanVarianceCrossValidatorModel = {
        val schema = trainingDataset.schema
        val sparkSession = trainingDataset.sparkSession
        val est = $(estimator)
        val eval = $(evaluator)
        val epm = $(estimatorParamMaps)

        val splits = this.kFold(trainingDataset, validationDataset, $(this.numFolds), $(this.seed))

        val metrics = splits.zipWithIndex.map { case ((training, validation), splitIndex) =>
            val trainingDF = sparkSession.createDataFrame(training, schema).cache()
            val validationDF = sparkSession.createDataFrame(validation, schema).cache()

            val foldMetrics = epm.zipWithIndex.map { case (paramMap, paramIndex) =>
                val model = est.fit(trainingDF, paramMap).asInstanceOf[MeanVarianceModel]
                val metric = eval.evaluate(model.transform(validationDataset, paramMap))
                metric
            }

            trainingDataset.unpersist()
            validationDataset.unpersist()
            foldMetrics
        }.transpose.map(_.sum / $(this.numFolds))

        val (bestMetric, bestIndex) =
            if (eval.isLargerBetter) metrics.zipWithIndex.maxBy(_._1)
            else metrics.zipWithIndex.minBy(_._1)

        val bestModel = est.fit(trainingDataset, epm(bestIndex)).asInstanceOf[MeanVarianceModel]
        copyValues(new MeanVarianceCrossValidatorModel(this.uid, bestModel, metrics).setParent(this.asInstanceOf[Estimator[MeanVarianceCrossValidatorModel]]))
    }

    override def copy(extra: ParamMap): MeanVarianceCrossValidator = {
        val copied = defaultCopy(extra).asInstanceOf[MeanVarianceCrossValidator]

        if (copied.isDefined(this.estimator)) {
            copied.setEstimator(copied.getEstimator.copy(extra))
        }

        if (copied.isDefined(this.evaluator)) {
            copied.setEvaluator(copied.getEvaluator.copy(extra))
        }

        copied
    }
}

private[stream] class MeanVarianceCrossValidatorModel (
        override val uid: String,
        val bestModel: MeanVarianceModel,
        val avgMetrics: Array[Double])
    extends Model[MeanVarianceCrossValidatorModel] {

    override def transform(dataset: Dataset[_]): DataFrame = {
        this.bestModel.transform(dataset)
    }

    override def transformSchema(schema: StructType): StructType = {
        this.bestModel.transformSchema(schema)
    }

    override def copy(extra: ParamMap): MeanVarianceCrossValidatorModel = {
        val copied = new MeanVarianceCrossValidatorModel(
            this.uid,
            this.bestModel.copy(extra),
            this.avgMetrics.clone()
        )

        copyValues(copied, extra).setParent(parent)
    }
}
