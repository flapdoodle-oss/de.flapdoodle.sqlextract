package de.flapdoodle.sqlextract.config

import de.flapdoodle.sqlextract.filetypes.Attributes
import de.flapdoodle.sqlextract.filetypes.Toml
import de.flapdoodle.sqlextract.filetypes.Yaml
import java.nio.file.Files
import java.nio.file.Path

data class DatabaseConnection(
    val driver: Path,
    val className: String,
    val jdbcUrl: String,
    val user: String?,
    val password: String?,
) {
    companion object {
        fun parse(basePath: Path, source: Attributes.Node): DatabaseConnection {
            val driverPath = source.values("driver", String::class).singleOrNull()
            val className = source.values("className", String::class).singleOrNull()
            val jdbcUrl = source.values("jdbcUrl", String::class).singleOrNull()
            val user = source.values("user", String::class).singleOrNull()
            val password = source.values("password", String::class).singleOrNull()

            require(driverPath != null) { "driver not set" }
            require(className != null) { "className not set" }
            require(jdbcUrl != null) { "className not set" }

            return DatabaseConnection(
                driver = basePath.resolve(driverPath),
                className = className,
                jdbcUrl = jdbcUrl,
                user = user,
                password = password
            )
        }


        fun parse(source: Path): DatabaseConnection {
            return IO.read(source) {
                parse(source.parent, it);
            }
        }
    }
}