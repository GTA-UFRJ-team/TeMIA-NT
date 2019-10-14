package br.ufrj.gta.stream.metrics

import scala.collection.mutable.{ListBuffer, Stack}

import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener, StreamingQueryProgress}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

class StreamingMetrics(names: Array[String]) extends Metrics(names) {
    val progress = new ListBuffer[StreamingQueryProgress]()

    var lastBatchId = -1L
    var streamingQueryProgress = new Stack[StreamingQueryProgress]()

    val listener = new StreamingQueryListener() {
        override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {}

        override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {}

        override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
            val curProgress = queryProgress.progress

            if (lastBatchId != -1L && lastBatchId != curProgress.batchId) {
                val lastStreamingQueryProgress = streamingQueryProgress.pop

                progress.append(lastStreamingQueryProgress)
                // Super method
                add(getMetrics(lastStreamingQueryProgress))

                streamingQueryProgress.clear
            }

            lastBatchId = curProgress.batchId
            streamingQueryProgress.push(curProgress)
        }
    }

    def getListener: StreamingQueryListener = this.listener

    def getProgress: ListBuffer[StreamingQueryProgress] = this.progress

    def getMetrics(progress: StreamingQueryProgress): Map[String, Any] = {
        val batchId = progress.batchId

        val timestamp = progress.timestamp

        val numInputRows = progress.numInputRows
        val inputRowsPerSecond = progress.inputRowsPerSecond
        val processedRowsPerSecond = progress.processedRowsPerSecond

        val triggerExecution = Option(progress.durationMs.get("triggerExecution")) match { case Some(_) => progress.durationMs.get("triggerExecution") case None => 0 }
        val queryPlanning = Option(progress.durationMs.get("queryPlanning")) match { case Some(_) => progress.durationMs.get("queryPlanning") case None => 0 }
        val getBatch = Option(progress.durationMs.get("getBatch")) match { case Some(_) => progress.durationMs.get("getBatch") case None => 0 }
        val getEndOffset = Option(progress.durationMs.get("getEndOffset")) match { case Some(_) => progress.durationMs.get("getEndOffset") case None => 0 }
        val setOffsetRange = Option(progress.durationMs.get("setOffsetRange")) match { case Some(_) => progress.durationMs.get("setOffsetRange") case None => 0 }
        val walCommit = Option(progress.durationMs.get("walCommit")) match { case Some(_) => progress.durationMs.get("walCommit") case None => 0 }
        val addBatch = Option(progress.durationMs.get("addBatch")) match { case Some(_) => progress.durationMs.get("addBatch") case None => 0 }
        val runContinuous = Option(progress.durationMs.get("runContinuous")) match { case Some(_) => progress.durationMs.get("runContinuous") case None => 0 }

        Map(
            "Batch id" -> batchId,
            "Timestamp" -> timestamp,
            "Input rows" -> numInputRows,
            "Input rows per second" -> inputRowsPerSecond,
            "Processed rows per second" -> processedRowsPerSecond,
            "Trigger execution" -> triggerExecution,
            "Query planning" -> queryPlanning,
            "Get batch" -> getBatch,
            "Get end offset" -> getEndOffset,
            "Set offset range" -> setOffsetRange,
            "WAL commit" -> walCommit,
            "Add batch" -> addBatch,
            "Run continuous" -> runContinuous
        )
    }
}

object StreamingMetrics {
    val names = Array("Batch id",
        "Timestamp",
        "Input rows",
        "Processed rows per second",
        "Trigger execution",
        "Query planning",
        "Get batch",
        "Get end offset",
        "Set offset range",
        "WAL commit",
        "Add batch",
        "Run continuous")
}
