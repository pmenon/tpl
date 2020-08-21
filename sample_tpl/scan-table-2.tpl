// SELECT COUNT(*) FROM test_1 WHERE cola >= 50 AND colb < 1000000
// cola is monotonically increasing, and colb is uniform in [0,10]
// So we should see a total of N-50 results where N is the total
// number of tuples in test_1. As of now, N=20000, so expect 19950.
fun main(execCtx: *ExecutionContext) -> int {
    var ret = 0
    var tvi: TableVectorIterator
    for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi); ) {
        var vpi = @tableIterGetVPI(&tvi)
        for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
            var cola = @vpiGetInt(vpi, 0)
            var colb = @vpiGetInt(vpi, 1)
            if (cola >= 50 and colb < 10000000) {
                ret = ret + 1
            }
        }
        @vpiReset(vpi)
    }
    @tableIterClose(&tvi)
    return ret
}
