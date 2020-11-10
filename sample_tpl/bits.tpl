fun main() -> int32 {
    // -----------------------------------------------------
    // Check @cttz() for different input types.
    // -----------------------------------------------------
    var a: uint8 = 2
    if (@cttz(a) != 1) {
    return 10
    }

    var b: uint16 = 4
    if (@cttz(b) != 2) {
    return 20
    }

    var c: uint32 = 5
    if (@cttz(c) != 0) {
    return 30
    }

    // -----------------------------------------------------
    // Check @ctlz().
    // -----------------------------------------------------
    var d: uint8 = 0
    if (@ctlz(d) != 8) {
    return 40
    }

    // @ctlz(c<<2) = @ctlz(5<<2) = @ctlz(20) = @ctlz(0b10100) = 32-5 = 27.
    if (@ctlz(c << 2) != 27) {
    return 50
    }

    return 0
}