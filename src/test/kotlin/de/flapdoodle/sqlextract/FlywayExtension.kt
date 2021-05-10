package de.flapdoodle.sqlextract

import org.flywaydb.core.Flyway
import org.flywaydb.core.api.configuration.Configuration
import org.junit.jupiter.api.extension.*
import java.sql.Connection
import java.sql.Savepoint

class FlywayExtension : BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {
    private val testConnection = TestConnection.default()
    private var connection: Connection? = null
    private var savepoint: Savepoint? = null

    override fun beforeAll(context: ExtensionContext) {
        connection = testConnection.connect().apply {
            autoCommit = false
        }

        require(context.testClass.isPresent) {"no test class"}
        val testClass = context.testClass.get();
        val location = testClass.packageName.replace(".", "/")

        println("textClass -> "+location)

        val flyway = Flyway(Flyway.configure()
            .locations(location)
            .sqlMigrationPrefix(testClass.simpleName)
            .dataSource(testConnection.jdbcUrl,testConnection.username, testConnection.password))
        flyway.migrate()
    }

    override fun afterAll(context: ExtensionContext) {
        connection = connection?.let {
            it.close()
            null
        }
    }

    override fun beforeEach(context: ExtensionContext) {
        savepoint = connection!!.setSavepoint("before");
    }

    override fun afterEach(context: ExtensionContext) {
        connection!!.rollback(savepoint)
        savepoint=null
    }


    class FlywayParameterResolver : ParameterResolver {
        override fun supportsParameter(parameterContext: ParameterContext, context: ExtensionContext): Boolean {
            return parameterContext.parameter.type
                .equals(Connection::class.java)
        }

        override fun resolveParameter(parameterContext: ParameterContext, context: ExtensionContext): Any {
            //return context.get
            return "WHAT?"
        }

    }
}