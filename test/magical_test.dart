import "package:unittest/unittest.dart";
import 'package:clean_data/clean_data.dart';
import "dart:async";

magicalTick(){
  new Timer.periodic(const Duration(milliseconds:1000), (t) {
    print('tick');
    return new List.filled(1000000, "x").join("joohoo");
  });
}

var data;

/*
 * enhance 8 level, 4 fork: 9s
 */
main() {
  enhance(Map m, num level){
    if (level==0) {
      return m['value'] = 'a';
    } else {
      for (int i=0; i<4; i++) {
        var mm = new Map();
        m['$i'] = mm;
        enhance(mm, level - 1);
      }
    }
  }
//  magicalTick();
  var m = {};
  enhance(m, 8);
  print(m.toString().length);
  var s = new Stopwatch()..start();
  var a = cleanify(m);
  a.dispose();
  print(s.elapsed);
  return new Future.delayed(new Duration(seconds: 500));
}


//main(){
//
//  new Timer.periodic(const Duration(milliseconds:1000), (t) {
//    print('tick');
//    return new List.filled(1000000, "x").join("joohoo");
//  });
//
//  String longString = (new List.filled(1000, 'a')).join('');
//  return new Future.delayed(new Duration(seconds: 1), (){
////  return new Future.sync((){
//    var j=0;
//    var l=[];
//    for(int i=0; i<1000000; i++){
//      j++;
//      if(i%1000 == 0){
//        print(j);
//      }
//      l.add(longString+'$i');
//    }
//  }).then((_){
//    return new Future.delayed(new Duration(minutes: 10), (){
//    });
//  });
//
//}

//  return Future.forEach(new List.filled(1, null), (_){
//  });

//    Map m = new Map.from({'a': {'a': longS}, 'b': longN});
//    Map m = new Map.from({'a': {'a': longS, 'b': {'a' : longS, 'b': longS}}});
//    Map m = new Map.from({'a': longS});


//        StreamController<dynamic> _controller =
//            new StreamController.broadcast(sync: true);
//        _controller.add('$longS $j');
//        var premenna = _controller.stream;
//        _controller.close();
//      var ref = new DataReference(m);

