package br.ufrj.gta.stream.schema

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

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

trait PacketSchema extends Schema {
    def getNumFields: Int = {
        this.getSchema.size
    }

    def getFieldsRange: Range = {
        0 until this.getNumFields
    }
}

trait FlowSchema extends Schema {
    def getLabelCol: String = {
        "label"
    }

    def getNumFeatures: Int

    def getFeaturesRange: Range = {
        0 until this.getNumFeatures
    }
}
