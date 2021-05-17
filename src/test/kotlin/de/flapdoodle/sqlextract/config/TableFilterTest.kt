package de.flapdoodle.sqlextract.config

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

internal class TableFilterTest {

    @ParameterizedTest
    @ValueSource(strings = ["FOO","FOO_"])
    fun validTableNameSamples(tableName: String) {
        assertThat(TableFilter.isValidTableName(tableName)).isTrue
    }

    @ParameterizedTest
    @ValueSource(strings = ["^FOO","(FOO)_"])
    fun invalidTableNameSamples(tableName: String) {
        assertThat(TableFilter.isValidTableName(tableName)).isFalse
    }

    @Test
    fun exactMatchMustOnlyMatchFullTableName() {
        val testee = TableFilter.Companion.ExactMatch("FOO")
        assertThat(testee.test("FOO")).isTrue
        assertThat(testee.test("FOOO")).isFalse
    }

    @Test
    fun patternMatchMustFindRegexInTableName() {
        val testee = TableFilter.Companion.PatternMatch("FOO_")
        assertThat(testee.test("FOO_")).isTrue
        assertThat(testee.test("FOO_BAR")).isTrue
        assertThat(testee.test("_FOO_BAR")).isTrue
        assertThat(testee.test("_FO_BAR")).isFalse
    }

    @Test
    fun validTableNameMustGiveExactMatch() {
        val matcher = TableFilter.matchNameOrRegex("EXACT_MATCH")
        
        assertThat(matcher)
            .isInstanceOf(TableFilter.Companion.ExactMatch::class.java)
            .isEqualTo(TableFilter.Companion.ExactMatch("EXACT_MATCH"))
    }

    @Test
    fun invalidTableNameIsHandlesAsRegex() {
        val matcher = TableFilter.matchNameOrRegex("^EXACT_MATCH")

        assertThat(matcher)
            .isInstanceOf(TableFilter.Companion.PatternMatch::class.java)
            .isEqualTo(TableFilter.Companion.PatternMatch("^EXACT_MATCH"))
    }


    @Test
    fun matchEveryTableBecauseNothingIsExcluded() {
        val testee = TableFilter(includes = emptySet(), excludes = emptySet())
        assertThat(testee.matchingTableName("FOO")).isTrue
    }

    @Test
    fun matchEveryIncludedTable() {
        val testee = TableFilter(includes = setOf("FOO","BAR"), excludes = emptySet())
        assertThat(testee.matchingTableName("FOO")).isTrue
        assertThat(testee.matchingTableName("BAR")).isTrue
        assertThat(testee.matchingTableName("BAZ")).isFalse
    }

    @Test
    fun dontMatchTableIfExcluded() {
        val testee = TableFilter(includes = setOf("FOO","BAR"), excludes = setOf("BAR"))
        assertThat(testee.matchingTableName("FOO")).isTrue
        assertThat(testee.matchingTableName("BAR")).isFalse
        assertThat(testee.matchingTableName("BAZ")).isFalse
    }
}