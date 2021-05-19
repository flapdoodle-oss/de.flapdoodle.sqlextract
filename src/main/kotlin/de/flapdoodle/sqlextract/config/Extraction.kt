package de.flapdoodle.sqlextract.config

import de.flapdoodle.sqlextract.filetypes.Attributes
import de.flapdoodle.sqlextract.filetypes.Toml
import de.flapdoodle.sqlextract.filetypes.Yaml
import java.nio.file.Files
import java.nio.file.Path

data class Extraction(
    val driver: Path,
    val className: String,
    val jdbcUrl: String,
    val user: String?,
    val password: String?,
    val dataSets: List<DataSet>,
    val foreignKeys: List<ForeignKeys>,
    val tableFilter: TableFilterList
) {
    companion object {
        fun parse(basePath: Path, source: Attributes.Node): Extraction {
            val driverPath = source.values("driver", String::class).singleOrNull()
            val className = source.values("className", String::class).singleOrNull()
            val jdbcUrl = source.values("jdbcUrl", String::class).singleOrNull()
            val user = source.values("user", String::class).singleOrNull()
            val password = source.values("password", String::class).singleOrNull()

            require(driverPath != null) { "driver not set" }
            require(className != null) { "className not set" }
            require(jdbcUrl != null) { "className not set" }

            val tableFilter = TableFilterList.parse(source.find("tables", Attributes.Node::class))

            val foreignKeys = ForeignKeys.parse(source.find("foreignKeys", Attributes.Node::class))

            val dataSetConfigs = source.find("dataset", Attributes.Node::class)

            require(dataSetConfigs != null) { "at least one dataset must be defined" }

            val dataSets = dataSetConfigs.nodeKeys().map {
                DataSet.parse(it, dataSetConfigs.get(it, Attributes.Node::class))
            }

            return Extraction(
                driver = basePath.resolve(driverPath),
                className = className,
                jdbcUrl = jdbcUrl,
                user = user,
                password = password,
                foreignKeys = foreignKeys,
                dataSets = dataSets,
                tableFilter = tableFilter
            )
        }


        fun parse(source: Path): Extraction {
            val content = String(Files.readAllBytes(source), Charsets.UTF_8)
            val fileName = source.fileName.toString()
            val configAsTree = when {
                fileName.endsWith(".toml") -> Toml.asTree(content)
                fileName.endsWith(".yaml") -> Yaml.asTree(content)
                else -> throw IllegalArgumentException("unsupported config file: $source")
            }
            println("-----------------------")
            println("config -> $configAsTree")
            println("-----------------------")

            return parse(source.parent, configAsTree)
        }
    }
}
