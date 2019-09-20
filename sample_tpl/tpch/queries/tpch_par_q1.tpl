// This is what the codegen looks like for now.
// It will likely change once I add vectorized operations.

struct Output {
  l_returnflag : StringVal
  l_linestatus : StringVal
  sum_qty : Real
  sum_base_price : Real
  sum_disc_price : Real
  sum_charge : Real
  avg_qty : Real
  avg_price : Real
  avg_disc : Real
  count_order : Integer
}

struct State {
  agg_table: AggregationHashTable
  sorter: Sorter
  count : int32 // debug
  execCtx : *ExecutionContext
}

struct ThreadState1 {
  ts_agg_table: AggregationHashTable
  filter: FilterManager
  ts_count : int32
}

struct ThreadState2 {
  ts_sorter: Sorter
}

struct AggValues {
  sum_qty : Real
  sum_base_price : Real
  sum_disc_price : Real
  sum_charge : Real
  avg_qty : Real
  avg_price : Real
  avg_disc : Real
  count_order : Integer
}

struct AggPayload {
  l_returnflag: StringVal
  l_linestatus: StringVal
  sum_qty : RealSumAggregate
  sum_base_price : RealSumAggregate
  sum_disc_price : RealSumAggregate
  sum_charge : RealSumAggregate
  avg_qty : RealAvgAggregate
  avg_price : RealAvgAggregate
  avg_disc : RealAvgAggregate
  count_order : CountAggregate
}

struct SorterRow {
  l_returnflag: StringVal
  l_linestatus: StringVal
  sum_qty : Real
  sum_base_price : Real
  sum_disc_price : Real
  sum_charge : Real
  avg_qty : Real
  avg_price : Real
  avg_disc : Real
  count_order : Integer
}

fun compareFn(lhs: *SorterRow, rhs: *SorterRow) -> int32 {
  if (lhs.l_returnflag < rhs.l_returnflag) {
    return -1
  }
  if (lhs.l_returnflag > rhs.l_returnflag) {
    return 1
  }
  if (lhs.l_linestatus < rhs.l_linestatus) {
    return -1
  }
  if (lhs.l_linestatus > rhs.l_linestatus) {
    return 1
  }
  return 0
}

fun p1Filter1(vec: *VectorProjectionIterator) -> int32 {
  var param = @dateToSql(1998, 12, 1)
  for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
    @vpiMatch(vec, @vpiGetDate(vec, 10) < param)
  }
  @vpiResetFiltered(vec)
  return 0
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggHTInit(&state.agg_table, @execCtxGetMem(execCtx), @sizeOf(AggPayload))
  @sorterInit(&state.sorter, @execCtxGetMem(execCtx), compareFn, @sizeOf(SorterRow))
  state.count = 0
  state.execCtx = execCtx
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggHTFree(&state.agg_table)
  @sorterFree(&state.sorter)
}

fun initTheadState1(execCtx: *ExecutionContext, ts: *ThreadState1) -> nil {
  @filterManagerInit(&ts.filter)
  @filterManagerInsertFilter(&ts.filter, p1Filter1)
  @filterManagerFinalize(&ts.filter)
  @aggHTInit(&ts.ts_agg_table, @execCtxGetMem(execCtx), @sizeOf(AggPayload))
  ts.ts_count = 0
}

fun teardownThreadState1(execCtx: *ExecutionContext, ts: *ThreadState1) -> nil {
  @filterManagerFree(&ts.filter)
  @aggHTFree(&ts.ts_agg_table)
}

fun initTheadState2(execCtx: *ExecutionContext, ts: *ThreadState2) -> nil {
  @sorterInit(&ts.ts_sorter, @execCtxGetMem(execCtx), compareFn, @sizeOf(SorterRow))
}

fun teardownThreadState2(execCtx: *ExecutionContext, ts: *ThreadState2) -> nil {
  @sorterFree(&ts.ts_sorter)
}

fun aggHashFn(iters: [*]*VectorProjectionIterator) -> uint64 {
  var vec = iters[0]
  return @hash(@vpiGetVarlen(vec, 8), @vpiGetVarlen(vec, 9))
}

fun constructAgg(agg_payload: *AggPayload, iters: [*]*VectorProjectionIterator) -> nil {
  var vec = iters[0]
  var nnn = @vpiGetVarlen(vec, 15)
  agg_payload.l_returnflag = @vpiGetVarlen(vec, 8)
  agg_payload.l_linestatus = @vpiGetVarlen(vec, 9)

  @aggInit(&agg_payload.sum_qty)
  @aggInit(&agg_payload.sum_base_price)
  @aggInit(&agg_payload.sum_disc_price)
  @aggInit(&agg_payload.sum_charge)
  @aggInit(&agg_payload.avg_qty)
  @aggInit(&agg_payload.avg_price)
  @aggInit(&agg_payload.avg_disc)
  @aggInit(&agg_payload.count_order)
}

fun constructAggFromPartial(agg_payload: *AggPayload, partial: *AggPayload) -> nil {
  agg_payload.l_linestatus = partial.l_linestatus
  agg_payload.l_returnflag = partial.l_returnflag
  @aggInit(&agg_payload.sum_qty)
  @aggInit(&agg_payload.sum_base_price)
  @aggInit(&agg_payload.sum_disc_price)
  @aggInit(&agg_payload.sum_charge)
  @aggInit(&agg_payload.avg_qty)
  @aggInit(&agg_payload.avg_price)
  @aggInit(&agg_payload.avg_disc)
  @aggInit(&agg_payload.count_order)
}



fun updateAgg(agg_payload: *AggPayload, iters: [*]*VectorProjectionIterator) -> nil {
  var vec = iters[0]
  var agg_values : AggValues
  agg_values.sum_qty = @vpiGetReal(vec, 4) // l_quantity
  agg_values.sum_base_price = @vpiGetReal(vec, 5) // l_extendedprice
  agg_values.sum_disc_price = @vpiGetReal(vec, 5) * @vpiGetReal(vec, 6) // l_extendedprice * l_discount
  agg_values.sum_charge = @vpiGetReal(vec, 5) * @vpiGetReal(vec, 6) * (@floatToSql(1.0) - @vpiGetReal(vec, 7)) // l_extendedprice * l_discount * (1- l_tax)
  agg_values.avg_qty = @vpiGetReal(vec, 4) // l_quantity
  agg_values.avg_price = @vpiGetReal(vec, 5) // l_extendedprice
  agg_values.avg_disc = @vpiGetReal(vec, 6) // l_discount
  agg_values.count_order = @intToSql(1)

  @aggAdvance(&agg_payload.sum_qty, &agg_values.sum_qty)
  @aggAdvance(&agg_payload.sum_base_price, &agg_values.sum_base_price)
  @aggAdvance(&agg_payload.sum_disc_price, &agg_values.sum_disc_price)
  @aggAdvance(&agg_payload.sum_charge, &agg_values.sum_charge)
  @aggAdvance(&agg_payload.avg_qty, &agg_values.avg_qty)
  @aggAdvance(&agg_payload.avg_price, &agg_values.avg_price)
  @aggAdvance(&agg_payload.avg_disc, &agg_values.avg_disc)
  @aggAdvance(&agg_payload.count_order, &agg_values.count_order)
}

fun updateAggFromPartial(agg_payload: *AggPayload, partial: *AggPayload) -> nil {
  @aggMerge(&agg_payload.sum_qty, &partial.sum_qty)
  @aggMerge(&agg_payload.sum_base_price, &partial.sum_base_price)
  @aggMerge(&agg_payload.sum_disc_price, &partial.sum_disc_price)
  @aggMerge(&agg_payload.sum_charge, &partial.sum_charge)
  @aggMerge(&agg_payload.avg_qty, &partial.avg_qty)
  @aggMerge(&agg_payload.avg_price, &partial.avg_price)
  @aggMerge(&agg_payload.avg_disc, &partial.avg_disc)
  @aggMerge(&agg_payload.count_order, &partial.count_order)
}


fun aggKeyCheck(agg_payload: *AggPayload, iters: [*]*VectorProjectionIterator) -> bool {
  var vec = iters[0]
  if (agg_payload.l_returnflag != @vpiGetVarlen(vec, 8)) {
    return false
  }
  if (agg_payload.l_linestatus != @vpiGetVarlen(vec, 9)) {
    return false
  }
  return true
}

fun aggKeyCheckPartial(agg_payload1: *AggPayload, agg_payload2: *AggPayload) -> bool {
  if (agg_payload1.l_returnflag != agg_payload2.l_returnflag) {
    return false
  }
  if (agg_payload1.l_linestatus != agg_payload2.l_linestatus) {
    return false
  }
  return true
}


fun worker1(state: *State, ts: *ThreadState1, l_tvi: *TableVectorIterator) -> nil {
  // Pipeline 1 (Aggregating)
  var x = 0
  var iters: [1]*VectorProjectionIterator
  for (@tableIterAdvance(l_tvi)) {
    var vec = @tableIterGetVPI(l_tvi)
    @filtersRun(&ts.filter, vec)
    iters[0] = vec
    @aggHTProcessBatch(&ts.ts_agg_table, &iters, aggHashFn, aggKeyCheck, constructAgg, updateAgg, true)
  }
}

fun mergerPartitions1(state: *State, agg_table: *AggregationHashTable, iter: *AHTOverflowPartitionIterator) -> nil {
  var x = 0
  for (; @aggPartIterHasNext(iter); @aggPartIterNext(iter)) {
    var partial_hash = @aggPartIterGetHash(iter)
    var partial = @ptrCast(*AggPayload, @aggPartIterGetRow(iter))
    var agg_payload = @ptrCast(*AggPayload, @aggHTLookup(agg_table, partial_hash, aggKeyCheckPartial, partial))
    if (agg_payload == nil) {
      agg_payload = @ptrCast(*AggPayload, @aggHTInsert(agg_table, partial_hash))
      constructAggFromPartial(agg_payload, partial)
    }
    updateAggFromPartial(agg_payload, partial)
  }
}

fun worker2(state: *State, ts: *ThreadState2, agg_table: *AggregationHashTable) -> nil {
  // Pipeline 2 (Sorting)
  var agg_iter: AHTIterator
  for (@aggHTIterInit(&agg_iter, agg_table); @aggHTIterHasNext(&agg_iter); @aggHTIterNext(&agg_iter)) {
    var agg_payload = @ptrCast(*AggPayload, @aggHTIterGetRow(&agg_iter))
    var sorter_row = @ptrCast(*SorterRow, @sorterInsert(&ts.ts_sorter))
    sorter_row.l_returnflag = agg_payload.l_returnflag
    sorter_row.l_linestatus = agg_payload.l_linestatus
    sorter_row.sum_qty = @aggResult(&agg_payload.sum_qty)
    sorter_row.sum_base_price = @aggResult(&agg_payload.sum_base_price)
    sorter_row.sum_disc_price = @aggResult(&agg_payload.sum_disc_price)
    sorter_row.sum_charge = @aggResult(&agg_payload.sum_charge)
    sorter_row.avg_qty = @aggResult(&agg_payload.avg_qty)
    sorter_row.avg_price = @aggResult(&agg_payload.avg_price)
    sorter_row.avg_disc = @aggResult(&agg_payload.avg_disc)
    sorter_row.count_order = @aggResult(&agg_payload.count_order)
  }
  @aggHTIterClose(&agg_iter)
}

fun pipeline3(execCtx: *ExecutionContext, state: *State) -> nil {
    // Pipeline 3 (Output to upper layers)
    var out: *Output
    var sort_iter: SorterIterator
    for (@sorterIterInit(&sort_iter, &state.sorter); @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter)) {
        out = @ptrCast(*Output, @outputAlloc(execCtx))
        var sorter_row = @ptrCast(*SorterRow, @sorterIterGetRow(&sort_iter))
        out.l_returnflag = sorter_row.l_returnflag
        out.l_linestatus = sorter_row.l_linestatus
        out.sum_qty = sorter_row.sum_qty
        out.sum_base_price = sorter_row.sum_base_price
        out.sum_disc_price = sorter_row.sum_disc_price
        out.sum_charge = sorter_row.sum_charge
        out.avg_qty = sorter_row.avg_qty
        out.avg_price = sorter_row.avg_price
        out.avg_disc = sorter_row.avg_disc
        out.count_order = sorter_row.count_order
    }
    @sorterIterClose(&sort_iter)
    @outputFinalize(execCtx)
}

fun main(execCtx: *ExecutionContext) -> int {
    var state: State
    // set up state
    setUpState(execCtx, &state)

    // Pipeline1
    var tls : ThreadStateContainer
    @tlsInit(&tls, @execCtxGetMem(execCtx))
    @tlsReset(&tls, @sizeOf(ThreadState1), initTheadState1, teardownThreadState1, execCtx)
    // parallel scan
    @iterateTableParallel("lineitem", &state, &tls, worker1)
    // Move thread-local states
    var aht_off: uint32 = 0
    @aggHTMoveParts(&state.agg_table, &tls, aht_off, mergerPartitions1)

    // Pipeline 2
    @tlsReset(&tls, @sizeOf(ThreadState2), initTheadState2, teardownThreadState2, execCtx)
    // Scan AHT and fill up sorters
    @aggHTParallelPartScan(&state.agg_table, &state, &tls, worker2)
    var sorter_off : uint32 = 0
    @sorterSortParallel(&state.sorter, &tls, sorter_off)

    // Output
    pipeline3(execCtx, &state)
    teardownState(execCtx, &state)
    return state.count
}
