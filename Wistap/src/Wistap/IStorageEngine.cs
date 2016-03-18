using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Wistap
{
    public interface IStorageEngine : IDisposable
    {
        Task Initialize();

        /// <summary>
        /// Create a new object.
        /// </summary>
        /// <param name="account"></param>
        /// <param name="type"></param>
        /// <param name="payload"></param>
        /// <returns></returns>
        Task<long> CreateObject(ByteString account, DataObjectType type, string payload);

        /// <summary>
        /// Update the payload of an existing object.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="payload"></param>
        /// <param name="version"></param>
        /// <returns></returns>
        Task<ByteString> UpdateObject(long id, string payload, ByteString version);

        /// <summary>
        /// Delete an existing object.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="version"></param>
        /// <returns></returns>
        Task DeleteObject(long id, ByteString version);

        /// <summary>
        /// Get a list of objects given their IDs.
        /// </summary>
        /// <param name="account"></param>
        /// <param name="ids"></param>
        /// <returns></returns>
        Task<IReadOnlyList<DataObject>> GetObjects(ByteString account, IEnumerable<long> ids);
    }
}
