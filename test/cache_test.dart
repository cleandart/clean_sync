library clean_sync.test.cache_test;

import 'dart:async';
import 'package:unittest/unittest.dart';
import 'package:clean_sync/profiling.dart';

main(){

  Cache cache;

  setUp((){
    cache = new Cache(new Duration(milliseconds: 100), 2);
  });

  test('cache basics', (){
    cache.put('a', 'A');
    expect(cache.get('a'), equals('A'));
    expect(cache.get('b'), isNull);
    return new Future.delayed(new Duration(milliseconds: 200), (){
      cache.put('b', 'B');
      expect(cache.get('a'), isNull);
      expect(cache.get('b'), equals('B'));
    });
  });

  test('putIfAbsent (hit)', (){
    cache.put('a', 'A');
    return cache.putIfAbsent('a', () => new Future.value(4))
    .then((val){
      expect(val, equals('A'));
    });
  });

  test('putIfAbsent (miss)', (){
    cache.put('a', 'A');
    return cache.putIfAbsent('b', () => new Future.value(4))
        .then((val){
          expect(val, equals(4));
        });
  });

  test('capacity', (){
    cache.put('a', 'A');
    cache.put('b', 'B');
    cache.put('c', 'C');
    cache.put('d', 'D');
    expect(cache.get('a'), isNull);
    expect(cache.get('b'), isNull);
    expect(cache.get('c'), 'C');
    expect(cache.get('d'), 'D');
  });

}
