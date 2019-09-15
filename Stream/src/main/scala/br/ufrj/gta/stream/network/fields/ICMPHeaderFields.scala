package br.ufrj.gta.stream.network.fields

import org.apache.spark.sql._
import org.apache.spark.sql.types._

object ICMPHeaderFields {
    def getSchema: StructType = {
        new StructType()
            .add("icmp_type", "int") // ICMP type
            .add("icmp_code", "int") // ICMP code
    }
}
