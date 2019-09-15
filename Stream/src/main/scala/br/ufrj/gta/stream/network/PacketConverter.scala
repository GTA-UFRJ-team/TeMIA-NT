package br.ufrj.gta.stream.network

import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

private[network] trait HasPacketConverterParams extends Params {
    private final val convertTCP = new BooleanParam(this, "convertTCP", "considers some TCP header fields when converting packets into flows")
    private final val convertUDP = new BooleanParam(this, "convertUDP", "considers some UDP header fields when converting packets into flows")
    private final val convertICMP = new BooleanParam(this, "convertICMP", "considers some ICMP header fields when converting packets into flows")

    setDefault(convertTCP -> true, convertUDP -> true, convertICMP -> true)

    def getConvertTCP: BooleanParam = this.convertTCP

    def getConvertUDP: BooleanParam = this.convertUDP

    def getConvertICMP: BooleanParam = this.convertICMP

    def setConvertTCP(value: Boolean): this.type = {
        set(this.convertTCP, value)
    }

    def setConvertUDP(value: Boolean): this.type = {
        set(this.convertUDP, value)
    }

    def getConvertICMP(value: Boolean): this.type = {
        set(this.convertICMP, value)
    }
}

abstract class PacketConverter extends Identifiable with HasPacketConverterParams {
    def convert(df: DataFrame): DataFrame
}
