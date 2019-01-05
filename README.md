# PgDoc

PgDoc is a library for using PostgreSQL as a JSON document store.

## Setup

Run the [SQL script in `src/PgDoc/SQL/pgdoc.sql`](src/PgDoc/SQL/pgdoc.sql) to create the required table and functions in the database.

## Document structure

Documents are stored in a single table with three columns:

* `body`: The JSON document itself. It can be any valid JSON object, including nested objects and nested arrays. This is a `jsonb` column, and it is possible to query and index any nested field. This column is `NULL` if the document has been deleted.
* `id`: The unique identifier of the document, of type `uuid`.
* `version`: The current version of the document. Any update to a document must specify the version being updated. This ensures documents can be read by the application, and later updated in a safe fashion.

In C#, documents are represented by the `Document` class:

```csharp
public class Document
{
    /// <summary>
    /// Gets the unique identifier of the document.
    /// </summary>
    public Guid Id { get; }

    /// <summary>
    /// Gets the JSON body of the document as a string, or null if the document does not exist.
    /// </summary>
    public string? Body { get; }

    /// <summary>
    /// Gets the current version of the document.
    /// </summary>
    public ByteString Version { get; }
}
```

## Deleting and creating documents

Retrieving a documents that doesn't exist will return a document with the `Body` property set to null. This can be either because the document has never been created, or because it has been deleted.

In addition, the `Version` property of a document that has never been created is always set to `ByteString.Empty`.

PgDoc has no concept of inserting or deleting. They are both treated as an update.

Creating a new document is equivalent to updating a document from a null body to a non-null body. Deleting a document is equivalent to updating a document from a non-null body to a null body.

## Usage

### Initialize the document store

```csharp
NpgsqlConnection databaseConnection = new NpgsqlConnection(connectionString);
IDocumentStore documentStore = new DocumentStore(databaseConnection);
await documentStore.Initialize();
```

### Create a new document

```csharp
Guid documentId = Guid.NewGuid();

Document newDocument = new Document(
    id: documentId,
    body: "{'key':'inital_value'}",
    version: ByteString.Empty);

await documentStore.UpdateDocuments(newDocument);
```

### Retrieve a document

```csharp
Document document = await documentStore.GetDocument(documentId);
```

### Update a document

```csharp
Document updatedDocument = new Document(
    id: document.Id,
    body: "{'key':'updated_value'}",
    version: document.Version);

await documentStore.UpdateDocuments(updatedDocument);
```

### Delete a document

```csharp
Document document = await documentStore.GetDocument(documentId);

Document deletedDocument = new Document(
    id: document.Id,
    body: null,
    version: document.Version);

await documentStore.UpdateDocuments(deletedDocument);
```

## License

Copyright 2016 Flavien Charlon

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
