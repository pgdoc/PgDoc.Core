using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Wistap
{
    public interface IStorageEngine : IDisposable
    {
        Task Initialize();

        /// <summary>
        /// Updates atomically the value of several objects.
        /// </summary>
        /// <param name="account">The account to which the objects belong.</param>
        /// <param name="updateObjects">The objects being updated.</param>
        /// <param name="checkObjects">The objects of which the versions are checked, but which are not updated.</param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        Task<ByteString> UpdateObjects(IEnumerable<Document> updateObjects, IEnumerable<Document> checkObjects);

        /// <summary>
        /// Gets a list of objects given their IDs.
        /// </summary>
        /// <param name="account">The account to which the objects belong.</param>
        /// <param name="ids">The IDs of the objects to retrieve.</param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        Task<IReadOnlyList<Document>> GetObjects(IEnumerable<DocumentId> ids);
    }
}
