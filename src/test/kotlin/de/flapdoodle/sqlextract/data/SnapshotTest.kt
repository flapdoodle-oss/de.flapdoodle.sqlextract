package de.flapdoodle.sqlextract.data

import de.flapdoodle.sqlextract.TableBuilder
import de.flapdoodle.sqlextract.graph.ForeignKeyAndReferenceGraph
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.sql.JDBCType

internal class SnapshotTest {

    @Test
    fun sample() {
        val service = TableBuilder("SERVICE")
            .column("ID", JDBCType.INTEGER)
            .primaryKey("ID","PK_ID")
            .build()

        val history = TableBuilder("HISTORY")
            .column("ID", JDBCType.INTEGER)
            .column("SERVICE_ID", JDBCType.INTEGER)
            .foreignKey("SERVICE_ID", "SERVICE", "ID")
            .primaryKey("ID","PK_ID")
            .build()

        val user = TableBuilder("USER")
            .column("ID", JDBCType.INTEGER)
            .column("MANDANT_ID", JDBCType.INTEGER)
            .primaryKey("ID","PK_ID")
            .primaryKey("MANDANT_ID","PK_MANDANT")
            .build()

        val card = TableBuilder("CARD")
            .column("ID", JDBCType.INTEGER)
            .column("HISTORY_ID", JDBCType.INTEGER)
            .column("USER_ID", JDBCType.INTEGER)
            .column("MANDANT_ID", JDBCType.INTEGER)
            .foreignKey("HISTORY_ID", "HISTORY", "ID")
            .foreignKey("USER_ID", "USER", "ID")
            .foreignKey("MANDANT_ID", "USER", "MANDANT_ID")
            .build()

        val tables = listOf(card,service,history,user)

        val graph = ForeignKeyAndReferenceGraph.of(tables)

        val snapshot = Snapshot(
            tableGraph = graph,
            tableMap = mapOf(
                card to listOf(Snapshot.Row(mapOf())),
                user to listOf(Snapshot.Row(mapOf()))
            )
        )

        println("--------------------")
        println(snapshot.tableRowsAsDot())
        println("--------------------")
    }
}