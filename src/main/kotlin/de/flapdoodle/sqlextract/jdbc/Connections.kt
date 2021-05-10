package de.flapdoodle.sqlextract.jdbc

import java.net.URL
import java.net.URLClassLoader
import java.nio.file.Path
import java.sql.Connection
import java.sql.Driver
import java.sql.DriverManager

object Connections {

    fun connection(
        jdbcUrl: String,
        className: String,
        username: String?,
        password: String?,
        classLoader: ClassLoader? = null
    ): Connection {
        if (classLoader != null) {
            val driver = Class.forName(className, true, classLoader)
                .getDeclaredConstructor()
                .newInstance() as Driver

            DriverManager.registerDriver(Wrapper(driver))
        } else {
            Class.forName(className)
        }

        return DriverManager.getConnection(jdbcUrl, username, password)
    }

    fun connection(
        jdbcUrl: String,
        className: String,
        username: String?,
        password: String?,
        driver: Path
    ): Connection {
        val driverUrl = "file://$driver"
        val driverClassPath = URL(driverUrl)
        val classLoader = URLClassLoader.newInstance(arrayOf(driverClassPath))

        return connection(jdbcUrl, className, username, password, classLoader)
    }

    class Wrapper(wrapped: Driver) : Driver by wrapped
}