package de.flapdoodle.sqlextract

import org.junit.jupiter.api.extension.*
import java.sql.Connection
import java.sql.Savepoint

class SqlInitExtension  : BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback,
    ParameterResolver {
    private val testConnection = TestConnection.default()
    private var connection: Connection? = null
    private var savepoint: Savepoint? = null

    override fun beforeAll(context: ExtensionContext) {
        require(context.testClass.isPresent) {"no test class"}

        val testClass = context.testClass.get();
        val sql = testClass.getResource(testClass.simpleName+".sql").readText()

        connection = testConnection.connect().apply {
            autoCommit = false

            prepareStatement(sql).execute()
        }
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


    override fun supportsParameter(parameterContext: ParameterContext, context: ExtensionContext): Boolean {
        return parameterContext.parameter.type
            .equals(Connection::class.java)
    }

    override fun resolveParameter(parameterContext: ParameterContext, context: ExtensionContext): Any {
        return connection!!
    }
}