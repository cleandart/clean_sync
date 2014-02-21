import 'dart:core';

clone(data) {
  if(data is List) {
    return new List.from(data.map((e) => clone(e)));
  }
  if(data is Map) {
    return new Map.fromIterables(data.keys, data.values.map((e) => clone(e)));
  }
  if(data is Set) {
    return new Set.from(data.map((e) => clone(e)));
  }
  return data;
}