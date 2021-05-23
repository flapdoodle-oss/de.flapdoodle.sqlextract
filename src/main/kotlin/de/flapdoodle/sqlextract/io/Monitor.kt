package de.flapdoodle.sqlextract.io

object Monitor {

    private val threadReporter = ThreadLocal<Reporter>()
    private val state = ThreadLocal<StateHolder>()

    fun <T> execute(reporter: Reporter = ConsoleReporter(), action: () -> T): T {
        try {
            threadReporter.set(reporter)
            state.set(StateHolder())
            return action()
        } finally {
            reporter.report("")
            state.remove()
            threadReporter.remove()
        }
    }

    fun <T> scope(key: String, action: () -> T): T {
        val stateHolder = state.get()

        return if (stateHolder!=null) {
            val lastState = stateHolder.start(key)
            try {
                return action()
            } finally {
                stateHolder.end(lastState)
            }
        } else {
            action()
        }

    }

    fun message(message: String) {
        state.get()?.let {
            it.message(message)
            threadReporter.get()?.let { reporter ->
                reporter.report(it.asMessage())
            }
        }
    }

    data class State(
        val key: String,
        val map: Map<String,String> = emptyMap()
    ) {
        fun message(message: String): State {
            return copy(map = map + (key to message))
        }

        fun asMessage(): String {
            return map.map { "${it.key}: ${it.value}" }.joinToString(separator = " | ")
        }
    }

    class StateHolder() {
        private var currentState=State("")

        fun start(key: String): State {
            val ret = currentState
            currentState = currentState.copy(key = key)
            return ret
        }

        fun end(state: State) {
            this.currentState=state
        }

        fun message(message: String) {
            currentState=currentState.message(message)
        }

        fun asMessage(): String {
            return currentState.asMessage()
        }
    }

    interface Reporter {
        fun report(message: String)

        companion object {
            inline operator fun invoke(crossinline delegate: (String) -> Unit): Reporter {
                return object : Reporter {
                    override fun report(message: String) {
                        delegate(message)
                    }
                }
            }
        }
    }

    class ConsoleReporter : Reporter {
        private var clearLastMessage = ""

        override fun report(message: String) {
            print(clearLastMessage)
            print("\r")
            print(message)
            print("\r")
            clearLastMessage = " ".repeat(message.length)
        }

    }
}