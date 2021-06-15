package de.flapdoodle.sqlextract.config

import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.filetypes.Toml
import de.flapdoodle.sqlextract.io.readResource
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class DataSetTest {

    @Test
    fun readSampleConfig() {
        val sample = javaClass.readResource("sample-dataset.toml")
        val tree = Toml.asTree(sample)
        val result = DataSet.parse("sample", tree)

        val expected = DataSet(
            name = "sample",
            table = Name("A", "PUBLIC"),
            where = listOf("ID = '123'"),
            limit = 10,
            orderBy = listOf("ID"),
            backtrack = listOf(
                Backtrack(
                    source = Name("B","PUBLIC"),
                    destination = Name("A","PUBLIC"),
                    where = listOf("NAME is not null"),
                    limit = 20,
                    orderBy = listOf("NAME desc")
                )
            )
        )

        assertThat(result).isEqualTo(expected)
    }
}