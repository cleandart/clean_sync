import 'dart:html';
import 'dart:async';
import "package:clean_data/clean_data.dart";
import "package:clean_sync/client.dart";
import "package:clean_ajax/client_browser.dart";

/**
 * Do not run this using DartEditor Launcher! It will not work due to same
 * origin policy. What to do: run dartium and follow this link:
 * http://0.0.0.0:8080/static/simple_example_client.html
 */

LIElement createListElement(person, persons) {
  LIElement li = new LIElement()
  ..className = "_id-${person["_id"]}"
  ..text = "#${person["_id"]} ${person["name"]} (${person["age"]})"
  ..dataset["_id"] = person["_id"]
  ..onClick.listen((MouseEvent event) {
    LIElement e = event.toElement;
    String _id = e.dataset["_id"];
    Data pers = persons.collection.firstWhere((d) => d["_id"] == _id);

    if (pers != null) {
      persons.collection.remove(pers);
    }
  });
  return li;
}

void main() {
  Subscription persons, persons24;

  // initialization of these Subscriptions
  Connection connection = createHttpConnection("http://0.0.0.0:8080/resources/",
      new Duration(milliseconds: 100));

  Subscriber subscriber = new Subscriber(connection);
  subscriber.init().then((_) {
    persons = subscriber.subscribe("persons");
    persons24 = subscriber.subscribe("personsOlderThan24");

    Map<String, Subscription> map = {
        '#list': persons,
        '#list24': persons24,
    };

    map.forEach((String sel, Subscription sub) {
      sub.collection.onChange.listen((event) {
        event.addedItems.forEach((person) {
          UListElement list = querySelector(sel);
          list.children.add(createListElement(person, sub));
        });
        event.removedItems.forEach((person) {
          querySelector('$sel > li._id-${person["_id"]}').remove();
        });
      });
    });

    querySelector('#send').onClick.listen((_) {
      InputElement name = querySelector("#name");
      InputElement age = querySelector("#age");

      persons.collection.add(new Data.from({
        "name" : name.value,
        "age" : int.parse(age.value)
      }));

      name.value = '';
      age.value = '';
    });
  });
}
