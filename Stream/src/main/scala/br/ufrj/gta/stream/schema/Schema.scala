package br.ufrj.gta.stream.schema

import org.apache.spark.sql._
import org.apache.spark.sql.types._

private[stream] trait Schema {
    def getSchema: StructType

    def getLabelCol: String

    def getNumFeatures: Int

    def getFeaturesRange: Range

    def featurize(df: DataFrame, featuresCol: String = "features"): DataFrame
}
