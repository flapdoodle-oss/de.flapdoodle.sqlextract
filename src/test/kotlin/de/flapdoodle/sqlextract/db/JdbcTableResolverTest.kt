package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.SqlInitExtension
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.sql.Connection
import java.sql.JDBCType

@ExtendWith(SqlInitExtension::class)
internal class JdbcTableResolverTest {

    @Test
    fun withPK(connection: Connection) {
        val testee = JdbcTableResolver(connection)
        val withPK = testee.byName(Name.parse("WITH_PK"))

        assertThat(withPK.name).isEqualTo("WITH_PK")

        assertThat(withPK.columns)
            .containsExactlyInAnyOrder(
                Column("ID", JDBCType.DECIMAL, false),
                Column("NAME", JDBCType.VARCHAR, false),
                Column("AGE", JDBCType.INTEGER, true)
            )

        assertThat(withPK.primaryKeys)
            .allMatch { it.columnName == "ID" }
            .size().isEqualTo(1)

        assertThat(withPK.foreignKeys).isEmpty()
    }

    @Test
    fun withFK(connection: Connection) {
        val testee = JdbcTableResolver(connection)
        val withFK = testee.byName(Name.parse("WITH_FK"))

        assertThat(withFK.name).isEqualTo("WITH_FK")

        assertThat(withFK.columns)
            .containsExactlyInAnyOrder(
                Column("ID", JDBCType.DECIMAL, false),
                Column("NAME", JDBCType.VARCHAR, false),
                Column("REF", JDBCType.DECIMAL, true)
            )

        assertThat(withFK.primaryKeys)
            .allMatch { it.columnName == "ID" }
            .size().isEqualTo(1)

        assertThat(withFK.foreignKeys)
            .containsExactlyInAnyOrder(
                ForeignKey("WITH_FK","REF","WITH_PK","ID")
            )
    }
}