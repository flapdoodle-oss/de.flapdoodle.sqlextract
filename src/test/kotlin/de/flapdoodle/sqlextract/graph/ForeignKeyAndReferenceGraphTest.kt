package de.flapdoodle.sqlextract.graph

import de.flapdoodle.sqlextract.TableBuilder
import de.flapdoodle.sqlextract.db.Name
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.sql.JDBCType

internal class ForeignKeyAndReferenceGraphTest {
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
            .primaryKey("ID","PK_ID")
            .build()

        val main = TableBuilder("MAIN")
            .column("ID", JDBCType.INTEGER)
            .column("DIRECT_REF_ID", JDBCType.INTEGER)
            .foreignKey("DIRECT_REF_ID", "DIRECT_REF", "ID")
            .primaryKey("ID","PK_ID")
            .build()

        val direct_ref = TableBuilder("DIRECT_REF")
            .column("ID", JDBCType.INTEGER, false)
            .column("NAME", JDBCType.VARCHAR, false)
            .primaryKey("ID","PK_ID")
            .primaryKey("NAME","PK_NAME")
            .build()

        val otherRoot = TableBuilder("OTHER_ROOT")
            .column("OTHER_ID", JDBCType.INTEGER)
            .foreignKey("OTHER_ID", "OTHER", "ID")
            .build()

        val other = TableBuilder("OTHER")
            .column("ID", JDBCType.INTEGER)
            .primaryKey("ID","PK_ID")
            .build()

        val tables = listOf(root, root_ref, main, direct_ref, otherRoot, other)

        val testee = ForeignKeyAndReferenceGraph.of(tables)

//        println(testee.asDot())
//        testee.dumpDebugInfo()

//        val filtered = testee.filter(Name("MAIN", "PUBLIC"))
//
//        println(filtered.asDot())
//
        assertThat(testee.foreignKeysTo(Name("MAIN", "PUBLIC")))
            .containsExactlyInAnyOrder(root.name)

        assertThat(testee.foreignKeysFrom(Name("MAIN", "PUBLIC")))
            .containsExactlyInAnyOrder(direct_ref.name)

        assertThat(testee.foreignKeysFrom(Name("ROOT", "PUBLIC")))
            .containsExactlyInAnyOrder(main.name, direct_ref.name, root_ref.name)
    }

    @Test
    fun graphMustProvideMatchinForeignKeyReferences() {

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

        val testee = ForeignKeyAndReferenceGraph.of(tables)

        assertThat(testee.foreignKeysTo(card.name)).isEmpty()

        assertThat(testee.foreignKeysFrom(card.name))
            .containsExactlyInAnyOrder(user.name, history.name)
    }
}