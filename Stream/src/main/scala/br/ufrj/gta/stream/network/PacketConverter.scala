package br.ufrj.gta.stream.network

import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

private[network] trait PacketConverterParams extends Params {
    private val convertTcpFields = new BooleanParam(this, "convertTcpFields", "considers some TCP header fields when converting packets into flows")
    private val convertUdpFields = new BooleanParam(this, "convertUdpFields", "considers some UDP header fields when converting packets into flows")
    private val convertIcmpFields = new BooleanParam(this, "convertIcmpFields", "considers some ICMP header fields when converting packets into flows")

    def getConvertTcpFields: BooleanParam = this.convertTcpFields

    def getConvertUdpFields: BooleanParam = this.convertUdpFields

    def getConvertIcmpFields: BooleanParam = this.convertIcmpFields

    def setConvertTcpFields(value: Boolean): this.type = {
        set(this.convertTcpFields, value)
    }

    def setConvertUdpFields(value: Boolean): this.type = {
        set(this.convertUdpFields, value)
    }

    def convertUseIcmpFields(value: Boolean): this.type = {
        set(this.convertIcmpFields, value)
    }
}

abstract class PacketConverter
        (override val uid: String)
    extends Identifiable with PacketConverterParams {

    def transform(df: DataFrame): DataFrame/* = {
        val ipNumCols = 8
        val tcpNumCols = 10
        val udpNumCols = 2
        val icmpNumCols = 2

        var lowerCols = 1
        var upperCols = ipNumCols

        val ipRange = lowerCols to upperCols
        lowerCols = upperCols

        upperCols += tcpNumCols
        val tcpRange = lowerCols to upperCols
        lowerCols = upperCols

        upperCols += udpNumCols
        val udpRange = lowerCols to upperCols
        lowerCols = upperCols

        upperCols += icmpNumCols
        val icmpRange = lowerCols to upperCols

        val columnDF = df
            .select(df("value").cast("string"))
            .withColumn("value", split(df("value"), ","))
            .select((0 to upperCols).map(c => df("value").getItem(c).as(s"col$c")))

        columnDF.persist()

        val ipDF = columnDF
            .select((ipRange).map(c => s"col$c"))

        val ipNumPackets = ipDF.count()

        ipDF.groupBy(ipDF("col1"), ipDF("col2"))
            .agg(
                avg(ipDF("col")),
                count(ipDF("col")),
                count(ipDF("col")),
                avg(ipDF("col"))
            )

        val ipRow = ipDF.collect()

        ipDF.unpersist()

        val tcpDF = columnDF
            .where(columnDF("col3") === "1")
            .select((tcpRange).map(c => s"col$c"))

        tcpDF.persist()

        val tcpNumPackets = tcpDF.count()

        tcpDF.groupBy(tcpDF("col1"), tcpDF("col2"))
            .agg(
                countDistinct(tcpDF("col3")),
                countDistinct(tcpDF("col4"))
            )

        val tcpRow = tcpDF.collect()

        tcpDF.unpersist()

        val udpDF = columnDF
            .where(columnDF("col3") === "1")
            .select((udpRange).map(c => s"col$c"))

        udpDF.persist()

        val udpNumPackets = udpDF.count()

        udpDF.groupBy(udpDF("col1"), udpDF("col2"))
            .agg(
                ,

            )

        val udpRow = udpDF.collect()

        udpDF.unpersist()

        val icmpDF = columnDF
            .where(columnDF("col3") === "1")
            .select((icmpRange).map(c => s"col$c"))

        icmpDF.persist()

        val icmpNumPackets = icmpDF.count()

        icmpDF.groupBy(icmpDF("col1"), icmpDF("col2"))
            .agg(
                countDistinct(icmpDF("col3")),
                countDistinct(icmpDF("col4"))
            )

        val icmpRow = icmpDF.collect()

        icmpDF.unpersist()

        columnDF.unpersist()
    }*/
}
