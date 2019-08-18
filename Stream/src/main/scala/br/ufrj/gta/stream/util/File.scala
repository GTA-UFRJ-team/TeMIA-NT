package br.ufrj.gta.stream.util

import java.io._
import java.nio.file.{Files, Paths}

object File {
    def appendSlash(path: String): String = {
        if (!path.endsWith("/")) {
            path + "/"
        } else {
            path
        }
    }

    def getPathnameFromFilename(filename: String): String = {
        filename.substring(0, filename.lastIndexOf("/"))
    }

    @throws(classOf[Exception])
    def createDirIfNotExists(path: String): Unit = {
        val p = Paths.get(path)

        if (!Files.exists(p)) {
            Files.createDirectories(p)
        }
    }

    @throws(classOf[Exception])
    def getFileReader(filename: String, encoding: String): BufferedReader = {
        new BufferedReader(new InputStreamReader(new FileInputStream(filename), encoding))
    }

    @throws(classOf[Exception])
    def getFileWriter(filename: String, encoding: String): BufferedWriter = {
        val path = File.getPathnameFromFilename(filename)

        File.createDirIfNotExists(path)
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
