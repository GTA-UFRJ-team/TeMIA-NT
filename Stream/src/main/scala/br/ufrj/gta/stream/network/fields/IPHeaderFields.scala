package br.ufrj.gta.stream.network.fields

import org.apache.spark.sql._
import org.apache.spark.sql.types._

object IPHeaderFields {
    def getSchema: StructType = {
        new StructType()
            .add("ip_srcaddr", "string") // (string) The source IP address
            .add("ip_dstaddr", "string") // (string) The destination IP address
            .add("ip_proto", "int") // The IP protocol (ie. TCP = 6, UDP = 17)
            .add("ip_ttl", "int") // IP time to travel
            .add("ip_flagdf", "int") // IP "do not frag" flag
            .add("ip_flagmf", "int") // IP "more frag" flag
            .add("ip_fragoffset", "int") // IP fragment offset
            .add("ip_hdrlen", "int") // IP header length
            .add("ip_pktlen", "int") // IP packet length
    }
}
