crowd-pulse-data-java
=====================

Java data access layer for Crowd Pulse.

---------------------

The available plugins are:

* `message-fetch` to load messages stored on a database
* `message-rx-fetch` to load messages stored on a database in a reactive way (experimental)
* `message-filter` to filter messages in a pipeline
* `message-persist` to save messages in a database
* `profile-fetch` to load profiles stored on a database
* `profile-rx-fetch` to load profiles stored on a database in a reactive way (experimental)
* `profile-persist` to save profiles in a database
* `streamer` to make the elements flow untouched in the stream

The included packages are:

* `entity`: POJO classes of Crowd Pulse entities
* `repository`: repositories for object persistence on MongoDB
* `plugin`: shared `IPlugin` implementations to fetch, filter and persist `Message`s and `Profile`s

## License

```
  Copyright 2015 Francesco Pontillo

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

```