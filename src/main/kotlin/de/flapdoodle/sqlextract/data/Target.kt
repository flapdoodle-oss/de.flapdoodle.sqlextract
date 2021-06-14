package de.flapdoodle.sqlextract.data

import java.nio.file.Path

data class Target(val path: Path) {
    init {
        require(path.toFile().isDirectory) {"target $path is not a directory"}
    }
    
    fun cacheFile(appendix: String): Path {
        return path.resolve("cache--$appendix")
    }

    fun dumpFile(): Path {
        return path.resolve("dump.sql")
    }

    fun tableDotFile(): Path {
        return path.resolve("tables.dot")
    }

    fun schemaDotFile(): Path {
        return path.resolve("schema.dot")
    }
}