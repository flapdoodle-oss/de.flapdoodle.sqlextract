package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.config.Extraction
import java.net.URL
import java.net.URLClassLoader
import java.sql.Connection
import java.sql.Driver
import java.sql.DriverManager


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

        val connection: Connection = DriverManager.getConnection(config.jdbcUrl, config.user, config.password)
        val tableResolver = JdbcTableResolver(connection)

        connection.use { connection ->
            config.dataSets.forEach { dataSet ->
                println("-> ${dataSet.name}")

                val table = tableResolver.byName(dataSet.table)

                val sqlQuery = "select * from ${dataSet.table} where ${dataSet.where}"
                println("query: $sqlQuery")
                val statement = connection.prepareStatement(sqlQuery)
                val resultSet = statement.executeQuery()
                while (resultSet.next()) {
                    println("-> "+resultSet)
                }
            }
//            val rs: ResultSet = it.metaData.getTables(null, null, "%", null)
//            while (rs.next()) {
//                println("-> "+rs.getString(3))
//            }
        }
    }

    class Wrapper(wrapped: Driver) : Driver by wrapped
}