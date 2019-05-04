fun sumElems(arr: [*]int32, x: int32, y: int32) -> int32 {
  return arr[x] + arr[y]
}

fun main() -> int32 {
  var arr: [10]int32
  for (var idx = 0; idx < 10; idx = idx + 1) {
    arr[idx] = idx
  }
  // arr[4] = 4, arr[5] = 5
  return sumElems(&arr, 4, 5)
}
