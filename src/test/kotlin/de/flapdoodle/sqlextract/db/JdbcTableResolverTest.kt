package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.SqlInitExtension
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.sql.Connection
import java.sql.JDBCType

@ExtendWith(SqlInitExtension::class)
internal class JdbcTableResolverTest {

    @Test
    fun foo(connection: Connection) {
        val testee = JdbcTableResolver(connection)
        val withPK = testee.byName("WITH_PK")

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
}