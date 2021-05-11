package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.SqlInitExtension
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.sql.Connection

@ExtendWith(SqlInitExtension::class)
internal class JdbcTableResolverTest {

    @Test
    fun foo(connection: Connection) {
        val testee = JdbcTableResolver(connection)
        val withPK = testee.byName("WITH_PK")
    }
}