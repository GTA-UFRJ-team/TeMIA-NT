package br.ufrj.gta.stream.network.fields

import org.apache.spark.sql._
import org.apache.spark.sql.types._

object IcmpHeaderFields {
    def getSchema: StructType = {
        new StructType()
            .add("type", "int") // ICMP type
            .add("code", "int") // ICMP code
    }
}
