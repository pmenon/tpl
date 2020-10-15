fun init(x: float32) -> Real {
    return @cos(@floatToSql(x) + @floatToSql(x))
}

fun main() -> int32 {
    var a = init(40.0)
    var b = init(1.0)

    if (a == b) {
        return 10
    }

    var c = init(40.0)

    if (a != c) {
        return 20
    }

    return 0
}