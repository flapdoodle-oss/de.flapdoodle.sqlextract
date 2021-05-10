package de.flapdoodle.sqlextract

import de.flapdoodle.sqlextract.jdbc.Connections
import java.sql.Connection
import java.util.*


data class TestConnection(
    val jdbcUrl: String,
    val driver: String,
    val username: String,
    val password: String,
) {
    fun connect(): Connection {
        return Connections.connection(
            jdbcUrl = jdbcUrl,
            className = driver,
            username = username,
            password = password
        )
    }

    companion object {
        fun default(): TestConnection {
            val props = Properties()
            props.load(TestConnection::class.java.getResourceAsStream("/jdbc.properties"))
            val jdbcUrl: String = props.getProperty("jdbc.url")
            val driver: String = props.getProperty("jdbc.driver")
            val username: String = props.getProperty("jdbc.user")
            val password: String = props.getProperty("jdbc.password")
            return TestConnection(jdbcUrl, driver, username, password)
        }
    }
}