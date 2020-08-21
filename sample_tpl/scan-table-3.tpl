// SELECT COUNT(*) FROM test_1 WHERE (cola >= 50 AND colb < 1000000) or (colc < 5000000)
// cola is monotonically increasing, colb is uniform in [0,9], and
// colc is uniform in [0,9999]. So, the condition is always true.
// The result should match the total tuple count in 'test_2'=20000.
fun main(execCtx: *ExecutionContext) -> int {
    var ret = 0
    var tvi: TableVectorIterator
    for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi); ) {
        var vpi = @tableIterGetVPI(&tvi)
        for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
            var cola = @vpiGetInt(vpi, 0)
            var colb = @vpiGetInt(vpi, 1)
            var colc = @vpiGetInt(vpi, 2)
            if ((cola >= 50 and colb < 10000000) or (colc < 500000)) {
                ret = ret + 1
            }
        }
        @vpiReset(vpi)
    }
    @tableIterClose(&tvi)
    return ret
}
