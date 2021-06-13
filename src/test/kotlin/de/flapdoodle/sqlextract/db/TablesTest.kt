package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.TableBuilder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.sql.JDBCType

internal class TablesTest {

    private val fooTable = TableBuilder("foo","s")
        .column("BAR_ID",JDBCType.INTEGER)
        .foreignKey("BAR_ID","bar","ID")
        .build()

    private val barTable = TableBuilder("bar","s")
        .column("BAZ_ID", JDBCType.INTEGER)
        .foreignKey("BAZ_ID", "baz", "ID")
        .build()

    private val bazTable = Table(
        name = Name.parse("s.baz")
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
            .containsExactly(Name.parse("s.baz"))
    }

    @Test
    fun resolveMissingTablesOnAdd() {
        val testee = Tables.empty().add(listOf<Name>(Name.parse("s.foo"),Name.parse("s.bar")), resolver)

        assertThat(testee.find(Name.parse("s.foo"))).isNotNull
        assertThat(testee.find(Name.parse("s.bar"))).isNotNull
        assertThat(testee.find(Name.parse("s.baz"))).isNotNull
    }
}