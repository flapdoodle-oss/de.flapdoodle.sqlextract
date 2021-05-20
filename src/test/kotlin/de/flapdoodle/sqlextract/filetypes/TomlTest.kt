package de.flapdoodle.sqlextract.filetypes

import de.flapdoodle.sqlextract.io.readResource
import org.junit.jupiter.api.Test

internal class TomlTest {

    @Test
    fun readSampleToml() {
        val sample = javaClass.readResource("sample.toml")
        val tree = Toml.asTree(sample)

        println("tree: $tree")
    }
}