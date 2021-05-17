package de.flapdoodle.sqlextract.db

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class TablesTest {

    private val fooTable = Table(
        name = Name.parse("foo"), foreignKeys = setOf(
            ForeignKey("foo", "BAR_ID", "bar", "ID")
        )
    )

    private val barTable = Table(
        name = Name.parse("bar"), foreignKeys = setOf(
            ForeignKey("bar", "BAZ_ID", "baz", "ID")
        )
    )

    private val bazTable = Table(
        name = Name.parse("baz")
    )

    private val resolver: (name: Name) -> Table = { name ->
        when (name.name) {
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
            .containsExactly(Name.parse("baz"))
    }

    @Test
    fun resolveMissingTablesOnAdd() {
        val testee = Tables.empty().add(listOf<Name>(Name.parse("foo"),Name.parse("bar")), resolver)

        assertThat(testee.find(Name.parse("foo"))).isNotNull
        assertThat(testee.find(Name.parse("bar"))).isNotNull
        assertThat(testee.find(Name.parse("baz"))).isNotNull
    }
}