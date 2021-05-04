package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.config.Extraction
import java.net.URL
import java.net.URLClassLoader
import java.sql.Driver
import java.sql.DriverManager
import java.sql.ResultSet


class Extractor {

    fun extract(config: Extraction) {
        val classLoader = URLClassLoader.newInstance(arrayOf(URL("jar:file:" + config.driver)))

        val driver = Class.forName(config.className, true, classLoader)
            .getDeclaredConstructor()
            .newInstance() as Driver

        DriverManager.registerDriver(driver)

        val connection = DriverManager.getConnection(config.jdbcUrl, config.jdbcUrl, config.jdbcUrl)
        connection.use {
            val rs: ResultSet = it.metaData.getTables(null, null, "%", null)
            while (rs.next()) {
                println("-> "+rs.getString(3))
            }
        }
    }
}