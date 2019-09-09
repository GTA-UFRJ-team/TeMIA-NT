package br.ufrj.gta.stream.util

private[util] class Config {
    var properties = Map.empty[String, String]

    def this(filename: String, encoding: String, commentToken: String) = {
        this()

        try {
            val fr = File.getFileReader(filename, encoding)

            var tmp = this.properties
            var line = fr.readLine()

            while (line != null) {
                if (!line.trim.isEmpty && !line.startsWith(commentToken)) {
                    val keyValue = line.split("=")
                    this.properties = this.properties + (keyValue(0).trim() -> keyValue(1).trim())
                }

                line = fr.readLine()
            }

            fr.close()
        } catch {
            case e: Throwable => {
                println("Could not open/read config file")
                e.printStackTrace
            }
        }
    }

    def contains(param: String): Boolean = this.properties.contains(param)

    def get(param: String): String = this.properties(param)

    def getAll: Map[String, String] = this.properties

    def set(param: String, value: String): Config = {
        this.properties = this.properties + (param -> value)

        this
    }
}

object Config {
    def empty: Config = {
        new Config()
    }

    def load(filename: String, encoding: String = "utf-8", commentToken: String = "#"): Config = {
        new Config(filename, encoding, commentToken)
    }
}
