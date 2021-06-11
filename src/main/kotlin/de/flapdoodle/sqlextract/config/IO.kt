package de.flapdoodle.sqlextract.config

import de.flapdoodle.sqlextract.filetypes.Attributes
import de.flapdoodle.sqlextract.filetypes.Toml
import de.flapdoodle.sqlextract.filetypes.Yaml
import java.nio.file.Files
import java.nio.file.Path

object IO {
    fun read(source: Path): Attributes.Node {
        val content = String(Files.readAllBytes(source), Charsets.UTF_8)
        val fileName = source.fileName.toString()
        val configAsTree = when {
            fileName.endsWith(".toml") -> Toml.asTree(content)
            fileName.endsWith(".yaml") -> Yaml.asTree(content)
            else -> throw IllegalArgumentException("unsupported config file: $source")
        }
        return configAsTree
    }

    fun <T> read(source: Path, transformation: (Attributes.Node) -> T): T {
        return transformation(read(source))        
    }
}