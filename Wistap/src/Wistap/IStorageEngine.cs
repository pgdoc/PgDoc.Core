using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace Wistap
{
    public interface IStorageEngine : IDisposable
    {
        Task Initialize();

        /// <summary>
        /// Update the payload of an existing object.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="payload"></param>
        /// <param name="version"></param>
        /// <returns></returns>
        Task<ByteString> UpdateObjects(ByteString accounts, IEnumerable<DataObject> objects);

        /// <summary>
        /// Get a list of objects given their IDs.
        /// </summary>
        /// <param name="account"></param>
        /// <param name="ids"></param>
        /// <returns></returns>
        Task<IReadOnlyList<DataObject>> GetObjects(ByteString account, IEnumerable<ObjectId> ids);

        DbTransaction StartTransaction();
    }
}
