package de.flapdoodle.sqlextract.graph

import de.flapdoodle.sqlextract.TableBuilder
import de.flapdoodle.sqlextract.db.Name
import org.junit.jupiter.api.Test
import java.sql.JDBCType

internal class TableGraphTest {

    private val schema = "PUBLIC"

    @Test
    fun graphForTable() {
        val root = TableBuilder("ROOT")
            .column("MAIN_ID", JDBCType.INTEGER)
            .column("DIRECT_REF_NAME", JDBCType.VARCHAR)
            .column("ROOT_REF_ID", JDBCType.INTEGER)
            .foreignKey("MAIN_ID", "MAIN", "ID")
            .foreignKey("DIRECT_REF_NAME", "DIRECT_REF", "NAME")
            .foreignKey("ROOT_REF_ID", "ROOT_REF", "ID")
            .build()

        val root_ref = TableBuilder("ROOT_REF")
            .column("ID", JDBCType.INTEGER, false)
            .build()

        val main = TableBuilder("MAIN")
            .column("ID", JDBCType.INTEGER)
            .column("DIRECT_REF_ID", JDBCType.INTEGER)
            .foreignKey("DIRECT_REF_ID", "DIRECT_REF", "ID")
            .build()

        val direct_ref = TableBuilder("DIRECT_REF")
            .column("ID", JDBCType.INTEGER, false)
            .column("NAME", JDBCType.VARCHAR, false)
            .build()

        val otherRoot = TableBuilder("OTHER_ROOT")
            .column("OTHER_ID", JDBCType.INTEGER)
            .foreignKey("OTHER_ID", "OTHER", "ID")
            .build()

        val other = TableBuilder("OTHER")
            .column("ID", JDBCType.INTEGER)
            .build()

        val tables = listOf(root, root_ref, main, direct_ref, otherRoot, other)

        val testee = TableGraph.of(tables)

//        println(testee.asDot())

        val filtered = testee.filter(Name("MAIN", "PUBLIC"))

        println(filtered.asDot())

        testee.referencesTo(Name("MAIN", "PUBLIC"))
            .forEach { println("$it -->") }

        testee.referencesFrom(Name("MAIN", "PUBLIC"))
            .forEach { println("--> $it") }
        
        testee.referencesFrom(Name("ROOT", "PUBLIC"))
            .forEach { println("--> $it") }
    }
}