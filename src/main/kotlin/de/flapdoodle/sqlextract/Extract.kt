package de.flapdoodle.sqlextract

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.context
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.validate
import com.github.ajalt.clikt.parameters.types.path
import de.flapdoodle.sqlextract.config.Extraction
import de.flapdoodle.sqlextract.db.Extractor

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

        override fun run() {
            val config = Extraction.parse(configPath)
            Extractor().extract(config)
        }
    }

    @JvmStatic
    fun main(args: Array<String>) {
        Args().main(args.toList())
    }
}