package types

fun <T> List<T>.asSet(): Set<T> {
    val set = toSet()
    require(set.size==size) {"value collision: $this"}
    return set
}

fun <T> List<T>.head(): T {
    require(size>0) {"list is empty"}
    return this[0]
}

fun <T> List<T>.tail(): List<T> {
    require(size>0) {"list is empty"}
    return subList(1,size)
}
