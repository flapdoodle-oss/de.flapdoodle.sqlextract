package de.flapdoodle.sqlextract.graph

import de.flapdoodle.sqlextract.db.Column
import de.flapdoodle.sqlextract.db.ForeignKey
import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.db.Table
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.sql.JDBCType

internal class TableGraphTest {

    private val schema = "PUBLIC"

    @Test
    fun graphForTable() {
        val first = Table(
            name = Name("FIRST", schema),
            columns = setOf(Column("SECOND_ID", JDBCType.INTEGER, false)),
            foreignKeys = setOf(ForeignKey("FIRST", "SECOND_ID", "SECOND", "ID"))
        )

        val second = Table(
            name = Name("SECOND", schema),
            columns = setOf(
                Column("ID", JDBCType.INTEGER, false),
                Column("THIRD_ID", JDBCType.INTEGER, false)
            ),
            foreignKeys = setOf(ForeignKey("SECOND", "THIRD_ID", "THIRD", "ID"))
        )

        val third = Table(
            name = Name("THIRD", schema),
            columns = setOf(
                Column("ID", JDBCType.INTEGER, false),
            ),
        )

        val tables = listOf(first,second,third)

        val testee = TableGraph(tables)

        println(testee.asDot())
    }
}