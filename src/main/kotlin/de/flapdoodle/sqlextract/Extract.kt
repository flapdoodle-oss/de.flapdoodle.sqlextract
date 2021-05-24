package de.flapdoodle.sqlextract

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.context
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.validate
import com.github.ajalt.clikt.parameters.types.path
import de.flapdoodle.sqlextract.config.Extraction
import de.flapdoodle.sqlextract.db.Extractor
import de.flapdoodle.sqlextract.io.Monitor
import java.nio.file.Files
import java.nio.file.attribute.PosixFileAttributes

object Extract {
    class Args : CliktCommand() {
        init {
            context {
                allowInterspersedArgs = false
            }
        }

        val configPath by argument("config").path(
            mustExist = true,
            canBeFile = true,
            canBeDir = false
        ).validate {
            require(it.toFile().isFile) { "is not a file"}
        }

        val target by argument("target").path(
            canBeFile = false,
            canBeDir = true
        ).validate {
            require(!it.toFile().exists() || it.toFile().isDirectory) { "is a file"}
        }

        override fun run() {
            if (!target.toFile().exists()) {
                Files.createDirectory(target)
            }
            
            val config = Extraction.parse(configPath.toAbsolutePath())
            Monitor.execute {
                Extractor().extract(config, target)
            }
        }
    }

    @JvmStatic
    fun main(args: Array<String>) {
        Args().main(args.toList())
    }
}