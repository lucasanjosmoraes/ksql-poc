# ksql-poc

POC of an app integrated with [ksqlDB](https://ksqldb.io). It follows the examples from the [Java API Client docs](https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-clients/java-client/).

## Usage

There are 2 components: `base-resources` is responsible to create the stream and the table that will be used by the app
and preloading them with data; `emulate-app` is the app that will query data from `ksql`.

### From command line

```sh
lein base-resources

lein emulate-app
```

### From Intellij

There are scripts in the directory `.run` that allow you to run either components. You just need to choose one of them
from the dropdown menu and press play 🛀🏽.

## License

Copyright © 2022 Lucas dos Anjos Moraes

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
