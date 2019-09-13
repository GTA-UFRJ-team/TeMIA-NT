package br.ufrj.gta.stream.network.fields

import org.apache.spark.sql._
import org.apache.spark.sql.types._

object IpHeaderFields {
    def getSchema: StructType = {
        new StructType()
            .add("srcaddr", "string") // (string) The source ip address
            .add("dstaddr", "string") // (string) The destination ip address
            .add("protocol", "int") // The protocol (ie. TCP = 6, UDP = 17)
            .add("ttl", "int") // IP time to travel
            .add("dfflag", "int") // "Do not frag" flag
            .add("mfflag", "int") // "More frag" flag
            .add("fragoffset", "int") // Fragment offset
            .add("hdrlen", "int") // IP header length
            .add("pktlen", "int") // IP packet length
    }
}
