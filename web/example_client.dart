import 'dart:html';
import 'dart:async';
import 'dart:math';
import "package:clean_data/clean_data.dart";
import "package:clean_sync/client.dart";
import "package:clean_ajax/clean_ajax.dart";

void main() {

  Server server = new Server("http://localhost:8080/resources");
  Random random = new Random();
  Subscriber subscriber = new Subscriber(server, "author${random.nextInt(100)}");

  Subscription persons = subscriber.subscribe("persons");
  Subscription personsOlderThan24 = subscriber.subscribe("personsOlderThan24");

  persons.data.onChange.listen((event) {
    event.addedItems.forEach((person) {
      UListElement list = querySelector('#list');

      LIElement li = new LIElement()
        ..className = "_id-${person["_id"]}"
        ..text = "#${person["_id"]} ${person["name"]} (${person["age"]})"
        ..dataset["_id"] = person["_id"].toString()
        ..onClick.listen((MouseEvent event) {
          LIElement e = event.toElement;
          int _id = int.parse(e.dataset["_id"]);
          Data pers = persons.data.firstWhere((d) => d["_id"] == _id);

          if (pers != null) {
            persons.data.remove(pers);
          }
        });

      list.children.add(li);
    });

    event.removedItems.forEach((person) {
      querySelector('#list > li._id-${person["_id"]}').remove();
    });
  });

  personsOlderThan24.data.onChange.listen((event) {
    event.addedItems.forEach((person) {
      UListElement list = querySelector('#list2');

      LIElement li = new LIElement()
        ..className = "_id-${person["_id"]}"
        ..text = "#${person["_id"]} ${person["name"]} (${person["age"]})";

      list.children.add(li);
    });

    event.removedItems.forEach((person) {
      querySelector('#list2 > li._id-${person["_id"]}').remove();
    });
  });

  querySelector('#send').onClick.listen((_) {
    InputElement id = querySelector("#_id");
    InputElement name = querySelector("#name");
    InputElement age = querySelector("#age");

    persons.data.add(new Data.fromMap({
      "_id" : int.parse(id.value),
      "name" : name.value,
      "age" : int.parse(age.value)
    }));

    name.value = '';
    age.value = '';
  });
}
