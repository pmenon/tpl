// Should produce 1000000 since half the boolean values are true.
fun main(execCtx: *ExecutionContext) -> int {
    var ret = 0
    var tvi: TableVectorIterator
    for (@tableIterInit(&tvi, "all_types"); @tableIterAdvance(&tvi); ) {
        var vpi = @tableIterGetVPI(&tvi)
        for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
            var a = @vpiGetBool(vpi, 0)
            var b = @vpiGetTinyInt(vpi, 1)
            var c = @vpiGetSmallInt(vpi, 2)
            var d = @vpiGetInt(vpi, 3)
            var e = @vpiGetBigInt(vpi, 4)
            var f = @vpiGetReal(vpi, 5)
            var g = @vpiGetDouble(vpi, 6)
            if (a == @boolToSql(true)) {
                ret = ret + 1
            }
        }
        @vpiReset(vpi)
    }
    @tableIterClose(&tvi)
    return ret
}
