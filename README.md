# PgDoc

Document stores offer a lot of flexibility in terms of data structures, however they rarely support ACID transactions. On the other hand, SQL databases are typically ACID-compliant but require managing a SQL schema in addition to application code.

PgDoc is a library for using PostgreSQL as a generic document store. It leverages the ACID-compliance of PostgreSQL and provides the ability to update, create or modify multiple documents as part of atomic batches.

## License

Copyright 2016 Flavien Charlon

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
