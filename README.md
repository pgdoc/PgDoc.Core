# PgDoc
[![PgDoc](https://img.shields.io/nuget/v/PgDoc.svg?style=flat-square&color=blue&logo=nuget)](https://www.nuget.org/packages/PgDoc/)

PgDoc is a library for using PostgreSQL as a JSON document store.

## Setup

Run the [SQL script in `src/PgDoc/SQL/pgdoc.sql`](src/PgDoc/SQL/pgdoc.sql) to create the required table and functions in the database.

## Document structure

Documents are stored in a single table with three columns:

* `body`: The JSON data itself. It can include nested objects and nested arrays. This is a `jsonb` column, and it is possible to query and index any nested field. This column is `NULL` if the document has been deleted.
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
    public long Version { get; }
}
```

## Initialization

Start by creating an instance of the `DocumentStore` class, and calling the `Initialize` method.

```csharp
NpgsqlConnection databaseConnection = new NpgsqlConnection(connectionString);
IDocumentStore documentStore = new DocumentStore(databaseConnection);
await documentStore.Initialize();
```

## Retrieving a document

Use the `GetDocuments` method (or the `GetDocument` extension method) to retrieve one or more documents by ID.

```csharp
Document document = await documentStore.GetDocument(documentId);
```

Attempting to retrieve a document that doesn't exist will return a `Document` object with a `Body` property set to null. This can be either because the document has not been created yet, or because it has been deleted.

## Updating

Updating a document is done in three steps:

1. Retrieve the current document.
2. Update the document in the application.
3. Call `UpdateDocuments` to store the updated document in the database.

PgDoc relies on optimistic concurrency to guarantee consistency. When a document is updated, the version of the document being updated must be supplied. If it doesn't match the current version of the document, the update will fail and an `UpdateConflictException` will be thrown.

```csharp
// Retrieve the document to update
Document document = await documentStore.GetDocument(documentId);

// Create the new version of the document with an updated body
Document updatedDocument = new Document(
    id: document.Id,
    body: "{'key':'updated_value'}",
    version: document.Version);

await documentStore.UpdateDocuments(updatedDocument);
```

It is also possible to atomically update several documents at once by passing multiple documents to `UpdateDocuments`. If any of the documents fails the version check, none of the documents will be updated.

## Deleting and creating documents

PgDoc has no concept of inserting or deleting. They are both treated as an update.

Creating a new document is equivalent to updating a document from a null body to a non-null body. The initial value of the `Version` property of a document that has never been created is always `0`.

```csharp
// Generate a random ID for the new document
Guid documentId = Guid.NewGuid();

// Create the new document
Document newDocument = new Document(
    id: documentId,
    body: "{'key':'inital_value'}",
    version: 0);

await documentStore.UpdateDocuments(newDocument);
```

Deleting a document is equivalent to updating a document from a non-null body to a null body.

```csharp
// Retrieve the document to delete
Document document = await documentStore.GetDocument(documentId);

// Create the new version of the document with a null body
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
