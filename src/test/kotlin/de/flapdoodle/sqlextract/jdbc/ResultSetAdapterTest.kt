package de.flapdoodle.sqlextract.jdbc

import de.flapdoodle.sqlextract.SqlInitExtension
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.sql.Connection

@ExtendWith(SqlInitExtension::class)
internal class ResultSetAdapterTest {

    private val logger: Logger = LoggerFactory.getLogger(ResultSetAdapterTest::class.java)

    @Test
    fun readSample(connection: Connection) {
        val names = connection.query {
            prepareStatement("select * from SAMPLE").executeQuery()
        }.map {
            column("NAME", String::class)
        }

        assertThat(names).containsExactly("Klaus")
    }
}