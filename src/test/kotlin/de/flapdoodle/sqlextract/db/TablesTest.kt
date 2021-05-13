package de.flapdoodle.sqlextract.db

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class TablesTest {

    private val fooTable = Table(
        name = "foo", foreignKeys = setOf(
            ForeignKey("foo", "BAR_ID", "bar", "ID")
        )
    )

    private val barTable = Table(
        name = "bar", foreignKeys = setOf(
            ForeignKey("bar", "BAZ_ID", "baz", "ID")
        )
    )

    private val bazTable = Table(
        name = "baz"
    )

    private val resolver: (name: String) -> Table = { name ->
        when (name) {
            "foo" -> fooTable
            "bar" -> barTable
            "baz" -> bazTable
            else -> throw IllegalArgumentException("unknown table: $name")
        }
    }

    @Test
    fun missingTablesFromForeignKeyReferences() {
        val testee = Tables(
            listOf(
                fooTable,
                barTable
            )
        )

        assertThat(testee.missingTableDefinitions())
            .containsExactly("baz")
    }

    @Test
    fun resolveMissingTablesOnAdd() {
        val testee = Tables.empty().add(listOf("foo","bar"), resolver)

        assertThat(testee.find("foo")).isNotNull
        assertThat(testee.find("bar")).isNotNull
        assertThat(testee.find("baz")).isNotNull
    }
}