package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.config.Extraction
import java.net.URL
import java.net.URLClassLoader
import java.sql.Driver
import java.sql.DriverManager
import java.sql.ResultSet
import kotlin.io.path.createTempDirectory


class Extractor {

    fun extract(config: Extraction) {
        println("config: $config")

        val driverUrl = "file://" + config.driver
        val driverClassPath = URL(driverUrl)
        val classLoader = URLClassLoader.newInstance(arrayOf(driverClassPath))

        val driver = Class.forName(config.className, true, classLoader)
            .getDeclaredConstructor()
            .newInstance() as Driver

        DriverManager.registerDriver(Wrapper(driver))

        val connection = DriverManager.getConnection(config.jdbcUrl, config.user, config.password)
        connection.use {
            val rs: ResultSet = it.metaData.getTables(null, null, "%", null)
            while (rs.next()) {
                println("-> "+rs.getString(3))
            }
        }
    }

    class Wrapper(wrapped: Driver) : Driver by wrapped
}