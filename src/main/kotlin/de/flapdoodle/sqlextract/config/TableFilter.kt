package de.flapdoodle.sqlextract.config

import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.filetypes.Attributes
import java.util.function.Predicate
import java.util.regex.Pattern

data class TableFilter(
    val name: String,
    val schema: String,
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
        private val TABLE_NAME_REGEX = Pattern.compile("^[\\p{L}_][\\p{L}\\p{N}@$#_]{0,127}$")

        internal fun isValidTableName(name: String) = TABLE_NAME_REGEX.matcher(name).matches()

        internal fun matchNameOrRegex(tableNameOrRegex: String): Predicate<Name> {
            return if (isValidTableName(tableNameOrRegex)) ExactMatch(tableNameOrRegex)
            else PatternMatch(tableNameOrRegex)
        }

        data class ExactMatch(val tableName: String) : Predicate<Name> {
            override fun test(p0: Name) = p0.name == tableName
        }

        data class PatternMatch(val tableNameRegex: String) : Predicate<Name> {
            private val regex = Pattern.compile(tableNameRegex)
            override fun test(p0: Name) = regex.matcher(p0.name).find()
        }

        fun parse(name: String, source: Attributes.Node): TableFilter {
            val schema = source.values("schema", String::class).singleOrNull()
            val includes = source.findValues("include", String::class)?.toSet()
            val excludes = source.findValues("exclude", String::class)?.toSet()

            require(schema != null) { "schema not defined" }

            return TableFilter(
                name = name,
                schema = schema,
                includes = includes ?: emptySet(),
                excludes = excludes ?: emptySet()
            )
        }
    }
}