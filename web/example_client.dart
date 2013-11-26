import 'dart:html';
import 'dart:async';
import "package:clean_data/clean_data.dart";
import "package:clean_sync/client.dart";
import "package:clean_ajax/client_browser.dart";

/**
 * Do not run this using DartEditor Launcher! It will not work due to same
 * origin policy. What to do: run dartium and follow this link:
 * http://0.0.0.0:8080/static/example_client.html
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
  Subscription personsDiff, personsDiff24, personsData, personsData24;

  // initialization of these Subscriptions
  Connection connection = createHttpConnection("http://0.0.0.0:8080/resources/",
      new Duration(milliseconds: 100));
  String authorData = 'dataAll';
  String authorData24 = 'data24';
  DataCollection personsDataCol = new DataCollection();
  DataCollection personsDataCol24 = new DataCollection();
  Communicator communicator = new Communicator(connection, 'persons',
      (List<Map> data) {handleData(data, personsDataCol, authorData);}, null,
      'data');
  Communicator communicator24 = new Communicator(connection,
      'personsOlderThan24',
      (List<Map> data) {handleData(data, personsDataCol24, authorData24);},
      null, 'data');

  personsData = new Subscription.config('persons', personsDataCol, connection,
      communicator, authorData, new IdGenerator(authorData));
  personsData24 = new Subscription.config('personsOlderThan24',
      personsDataCol24, connection, communicator24, authorData24,
      new IdGenerator(authorData24));

  Subscriber subscriber = new Subscriber(connection);
  subscriber.init().then((_) {
    personsDiff = subscriber.subscribe("persons");
    personsDiff24 = subscriber.subscribe("personsOlderThan24");
    personsData.start();
    personsData24.start();

    Map<String, Subscription> map = {
        '#list-diff': personsDiff,
        '#list24-diff': personsDiff24,
        '#list-data': personsData,
        '#list24-data': personsData24,
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

      personsDiff.collection.add(new Data.from({
        "name" : name.value,
        "age" : int.parse(age.value)
      }));

      name.value = '';
      age.value = '';
    });
  });
}
