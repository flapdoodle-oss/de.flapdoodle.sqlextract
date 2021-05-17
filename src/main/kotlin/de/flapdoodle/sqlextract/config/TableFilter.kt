package de.flapdoodle.sqlextract.config

import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.filetypes.Attributes
import java.util.function.Predicate
import java.util.regex.Pattern

data class TableFilter(
    val includes: Set<String> = emptySet(),
    val excludes: Set<String> = emptySet()
) {
    private val includesMatcher: List<Predicate<Name>> = includes.map(::matchNameOrRegex)
    private val excludesMatcher: List<Predicate<Name>> = excludes.map(::matchNameOrRegex)

    fun matchingTableName(name: String): Boolean {
        return matchingTableName(Name.parse(name))
    }

    fun matchingTableName(name: Name): Boolean {
        val ret = ((includesMatcher.isEmpty() || includesMatcher.any { it.test(name) })
                && excludesMatcher.none { it.test(name) })
//        println("$name -> $ret")
        return ret
    }

    companion object {
        private val SCHEMA_NAME_REGEX = Pattern.compile("^[\\p{L}_][\\p{L}\\p{N}@$#_]{0,127}$")
        private val TABLE_NAME_REGEX = Pattern.compile("^[\\p{L}_][\\p{L}\\p{N}@$#_]{0,127}$")

        internal fun isValidTableName(name: String) = TABLE_NAME_REGEX.matcher(name).matches()
        internal fun isValidSchemaName(name: String) = SCHEMA_NAME_REGEX.matcher(name).matches()

        internal fun matchNameOrRegex(tableNameOrRegex: String): Predicate<Name> {
            val idx=tableNameOrRegex.indexOf('.')

            val (schema,tableName) = if (idx!=-1)
                tableNameOrRegex.substring(0,idx) to tableNameOrRegex.substring(idx)
            else
                null to tableNameOrRegex

            val tableMatcher = if (isValidTableName(tableName)) ExactMatch(tableName)
            else PatternMatch(tableName)
            val schemaMatcher = if (schema!=null) {
                if (isValidSchemaName(schema)) ExactMatchSchema(schema)
                else PatternMatchSchema(schema)
            } else {
               AnySchema
            }

            return And(tableMatcher,schemaMatcher)
        }

        data class And(val first: Predicate<Name>, val second: Predicate<Name>): Predicate<Name> {
            override fun test(p0: Name): Boolean {
                return first.test(p0) && second.test(p0)
            }
        }

        data class ExactMatch(val tableName: String) : Predicate<Name> {
            override fun test(p0: Name) = p0.name == tableName
        }

        data class ExactMatchSchema(val schema: String) : Predicate<Name> {
            override fun test(p0: Name) = p0.schema == schema
        }

        object AnySchema : Predicate<Name> {
            override fun test(p0: Name): Boolean = true
        }

        data class PatternMatch(val tableNameRegex: String) : Predicate<Name> {
            private val regex = Pattern.compile(tableNameRegex)
            override fun test(p0: Name) = regex.matcher(p0.name).find()
        }

        data class PatternMatchSchema(val schemaRegex: String) : Predicate<Name> {
            private val regex = Pattern.compile(schemaRegex)
            override fun test(p0: Name) = regex.matcher(p0.schema).find()
        }

        fun parse(source: Attributes.Node?): TableFilter {
            if (source != null) {
                val includes = source.findValues("include", String::class)?.toSet()
                val excludes = source.findValues("exclude", String::class)?.toSet()
                return TableFilter(
                    includes = includes ?: emptySet(),
                    excludes = excludes ?: emptySet()
                )
            }
            return TableFilter()
        }
    }
}