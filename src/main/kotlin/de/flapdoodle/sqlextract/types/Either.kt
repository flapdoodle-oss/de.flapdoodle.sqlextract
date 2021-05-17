package de.flapdoodle.sqlextract.types

sealed class Either<A, B> {
    class Left<A, B>(val left: A) : Either<A, B>()
    class Right<A, B>(val right: B) : Either<A, B>()

    companion object {
        fun <L, R> left(value: L): Either<L, R> {
            return Left(value)
        }

        fun <L, R> right(value: R): Either<L, R> {
            return Right(value)
        }
    }
}