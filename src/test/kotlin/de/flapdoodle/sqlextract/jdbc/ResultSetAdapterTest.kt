package de.flapdoodle.sqlextract.jdbc

import de.flapdoodle.sqlextract.FlywayExtension
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@ExtendWith(FlywayExtension::class)
internal class ResultSetAdapterTest {

    private val logger: Logger = LoggerFactory.getLogger(ResultSetAdapterTest::class.java)

    @Test
    fun readSample() {
        logger.info("Example log from {}", ResultSetAdapterTest::class.java.getSimpleName())
//        val testConnection = TestConnection.default()
//        testConnection.connect().use {
//            val location = ResultSetAdapterTest::class.java.packageName.replace(".", "/")
//
//            println("-> flyway location: $location")
//
//            val flyway = Flyway()
//            flyway.setLocations(location)
//            flyway.setDataSource(testConnection.jdbcUrl,testConnection.username, testConnection.password)
//            flyway.migrate()
//        }

        println("whooohoo")
    }
}