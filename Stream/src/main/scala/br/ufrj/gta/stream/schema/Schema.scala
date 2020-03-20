package br.ufrj.gta.stream.schema

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

// Base schema access methods
trait Schema {
    def getSchema: StructType

    def getNumCols: Int = {
        this.getSchema.size
    }

    def getColsRange: Range = {
        0 until this.getNumCols
    }

    def getColNames: Seq[String] = {
        this.getSchema.map[String, Seq[String]](sf => sf.name)
    }
}

// Schema access methods based on Antonio or flowtbag features
trait PacketSchema extends Schema {
    def getNumFields: Int = {
        this.getSchema.size
    }

    def getFieldsRange: Range = {
        0 until this.getNumFields
    }
}

// Schema access methods based on Spark abstraction
trait FlowSchema extends Schema {
    def getLabelCol: String = {
        "label"
    }

    def getNumFeatures: Int

    def getFeaturesRange: Range = {
        0 until this.getNumFeatures
    }
}
