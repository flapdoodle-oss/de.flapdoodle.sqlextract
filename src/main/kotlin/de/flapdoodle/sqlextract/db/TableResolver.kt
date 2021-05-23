package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.io.Monitor

fun interface TableResolver {
    fun byName(name: Name): Table

    fun withMonitor(): TableResolver {
        val that = this;
        return TableResolver {
            Monitor.scope("inspect $it") {
                that.byName(it)
            }
        }
    }

    fun withPostProcess(postProcess: (Table) -> Table = { it }): TableResolver {
        val that = this;
        return TableResolver {
            postProcess(that.byName(it))
        }
    }
}