using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Wistap
{
    public interface IDocumentStore : IDisposable
    {
        Task Initialize();

        /// <summary>
        /// Updates atomically the body of several objects.
        /// </summary>
        /// <param name="updatedDocuments">The documents being updated.</param>
        /// <param name="checkedDocuments">The documents of which the versions are checked, but which are not updated.</param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        Task<ByteString> UpdateDocuments(IEnumerable<Document> updatedDocuments, IEnumerable<Document> checkedDocuments);

        /// <summary>
        /// Gets a list of documents given their IDs.
        /// </summary>
        /// <param name="ids">The IDs of the documents to retrieve.</param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        Task<IReadOnlyList<Document>> GetDocuments(IEnumerable<DocumentId> ids);
    }
}
