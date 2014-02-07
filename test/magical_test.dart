import "package:unittest/unittest.dart";
import 'package:clean_data/clean_data.dart';
import "dart:async";
import 'dart:collection';

magicalTick(){
  new Timer.periodic(const Duration(milliseconds:1000), (t) {
    print('tick');
    return new List.filled(1000000, "x").join("joohoo");
  });
}

var data;

/*
 * enhance 8 level, 4 fork: 16s, 9s, 2.9s, 1.4s
 */

test1(){
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

  enhance2(Map m, num level){
    for (int i=0; i<40; i++) {
      if(level==0){
        m['$i'] = 'a';
      } else {
        var mm = new Map();
        m['$i'] = mm;
        enhance(mm, level - 1);
      }
    }
  }


//  magicalTick();
  var m = {};
  enhance(m, 7);
  print(m.toString().length);
  var s = new Stopwatch()..start();
  var a = cleanify(m);
  print(a);
  a.dispose();
  print(s.elapsed);
  return new Future.delayed(new Duration(seconds: 500));
}

test2(){
  String longString = (new List.filled(1000, 'a')).join('');
  return new Future.delayed(new Duration(seconds: 1), (){
//  return new Future.sync((){
    var j=0;
    var l=[];
    for(int i=0; i<1000000; i++){
      j++;
      if(i%1000 == 0){
        print(j);
      }
      l.add(longString+'$i');
    }
  }).then((_){
    return new Future.delayed(new Duration(minutes: 10), (){
    });
  });
}

var longS = new List.filled(1000, 'a').join('');

test3(){
    Map m = new Map.from({'a': {'a': longS, 'b': {'a' : longS, 'b': longS}}});
    for(int i=0; i<10000000000; i++){
      print(i);
      cleanify(m);
    }

}

/*
 *  num: 67m
 *  Map: 6m (empty)
 *  HashMap: 7m (empty)
 *  SplayTreeMap: 4m (empty)
 *  List: 9m (empty)
 *  StreamController.broadcast: 11m (sync nezalezi)
 *  StreamController 16m (sync nezalezi)
 *  TimerEvent: 6m
 *  empry func: 25m
 *
 *  DataMap (o mne, after improvment): 427k, 456k, 570k, 687k
 *  DataMap (o mne, before improvment): 123k
 *  Map (o mne, plain map): 1285k
 *  Map.toString (o mne): 7065k
 *
 *  o mne:
 *  {'meno': 'Tomas', 'adresa': 'gagarinova 47',
      'sex': 'male', 'iq': 'undefined', 'vek': '8',
      'rodicia': {'mama': 'ma', 'otec': 'tiez ma'}})
 *
 */
test4(){
  var l=[];
  num i=0;
  while(true){
//    Timer.run((){});
//    var ctrl = new StreamController(sync: true);
    i++;
    if (i%1000 == 0){
       print(i/1000);
    }
    l.add(new DataMap.from({'meno': 'Tomas', 'adresa': 'gagarinova 47',
      'sex': 'male', 'iq': 'undefined', 'vek': '8',
      'rodicia': {'mama': 'ma', 'otec': 'tiez ma'}}));
  }
}


main() {
  test1();
}



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

