// Copyright 2016 Flavien Charlon
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace PgDoc;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

/// <summary>
/// Represents an object used to retrieve and modify documents.
/// </summary>
public interface IDocumentStore : IDisposable
{
    /// <summary>
    /// Initializes the document store.
    /// </summary>
    /// <returns>The task object representing the asynchronous operation.</returns>
    Task Initialize();

    /// <summary>
    /// Updates atomically the body of multiple documents.
    /// </summary>
    /// <param name="updatedDocuments">The documents being updated.</param>
    /// <param name="checkedDocuments">The documents whose versions are checked, but which are not updated.</param>
    /// <returns>The task object representing the asynchronous operation.</returns>
    /// <exception cref="UpdateConflictException">Thrown when attempting to modify a document using the wrong
    /// base version.</exception>
    Task UpdateDocuments(IEnumerable<Document> updatedDocuments, IEnumerable<Document> checkedDocuments);

    /// <summary>
    /// Retrieves multiple documents given their IDs.
    /// </summary>
    /// <param name="ids">The IDs of the documents to retrieve.</param>
    /// <returns>The task object representing the asynchronous operation.</returns>
    Task<IReadOnlyList<Document>> GetDocuments(IEnumerable<Guid> ids);
}
