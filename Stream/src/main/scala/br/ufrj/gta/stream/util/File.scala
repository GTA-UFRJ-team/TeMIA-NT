package br.ufrj.gta.stream.util

import java.io._

object File {
    @throws(classOf[Exception])
    def getFileWriter(filename: String, encoding: String): BufferedWriter = {
        new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename), encoding))
    }

    def exportCSV(filename: String, header: Iterable[_], values: Iterable[Iterable[_]]): Unit = {
        try {
            val bw = File.getFileWriter(filename, "utf-8")

            bw.write(header.mkString(",") + "\n")
            for (value <- values) bw.write(value.mkString(",") + "\n")

            bw.close()
        } catch {
            case e: Throwable => {
                println("Could not export data into CSV file")
                e.printStackTrace
            }
        }
    }
}
