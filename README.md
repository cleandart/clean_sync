Server – client synchronization for web applications
=====================================================

[![Build Status](https://drone.io/github.com/cleandart/clean_sync/status.png)](https://drone.io/github.com/cleandart/clean_sync/latest)

Who likes boilerplate code?
-----------------------------

```dart

saveUserProfile(user) {
  HttpRequest.request(saveUserUrl, method: "POST", sendData: JSON.encode(user));
}

Future<User> loadUserProfile() {
  return HttpRequest.request(loadUserUrl, method: "GET")
      .then((data) => new User(JSON.decode(data)));
}

```

And now do not forget to call `saveUserProfile` whenever the profile changes on the client, to call periodically `loadUserProfile` to handle the profile changes on the server, to handle lost connection, and many others.

Let's the machine do the job!
------------------------------

Programming is about art, not about writting boilerplate code!

Wouldn't this be much simpler?

```dart
var userSubscription = subscriber.subscribe("users");
var users = userSubscription.collection;

displayUsers(users);

users.onChange((changeSet) => displayUsers(users));
```

Now, whenever the users change on the server, we redraw the screen. Writing works similarly easy!

```dart
var user = users.findBy("_id", 3);
user["email"] = "john.doe@example.com";
```

That is it!
