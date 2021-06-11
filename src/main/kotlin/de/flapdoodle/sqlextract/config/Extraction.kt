package de.flapdoodle.sqlextract.config

import de.flapdoodle.sqlextract.filetypes.Attributes
import de.flapdoodle.sqlextract.filetypes.Toml
import de.flapdoodle.sqlextract.filetypes.Yaml
import java.nio.file.Files
import java.nio.file.Path

data class Extraction(
    val databaseConnection: DatabaseConnection,
    val dataSets: List<DataSet>,
    val foreignKeys: List<ForeignKeys>,
    val tableFilter: TableFilterList
) {
    companion object {
        fun parse(basePath: Path, source: Attributes.Node): Extraction {
            val driverConfigPath = source.values("connection", String::class).singleOrNull()

            require(driverConfigPath != null) { "connection not set" }

            val databaseConnection = DatabaseConnection.parse(basePath.resolve(driverConfigPath))

            val tableFilter = TableFilterList.parse(source.find("tables", Attributes.Node::class))

            val foreignKeys = ForeignKeys.parse(source.find("foreignKeys", Attributes.Node::class))

            val dataSetConfigs = source.find("dataset", Attributes.Node::class)

            require(dataSetConfigs != null) { "at least one dataset must be defined" }

            val dataSets = dataSetConfigs.nodeKeys().map {
                DataSet.parse(it, dataSetConfigs.get(it, Attributes.Node::class))
            }

            return Extraction(
                databaseConnection = databaseConnection,
                foreignKeys = foreignKeys,
                dataSets = dataSets,
                tableFilter = tableFilter
            )
        }


        fun parse(source: Path): Extraction {
            return IO.read(source) {
                parse(source.parent, it)
            }
        }
    }
}
