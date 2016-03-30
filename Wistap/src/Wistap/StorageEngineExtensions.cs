using System.Collections.Generic;
using System.Threading.Tasks;

namespace Wistap
{
    public static class StorageEngineExtensions
    {
        public static Task<ByteString> UpdateObjects(this IStorageEngine storageEngine, params DataObject[] objects)
        {
            return storageEngine.UpdateObjects((IEnumerable<DataObject>)objects, (IEnumerable<DataObject>)new DataObject[0]);
        }

        public static Task<ByteString> UpdateObject(this IStorageEngine storageEngine, ObjectId id, string value, ByteString version)
        {
            return storageEngine.UpdateObjects(new DataObject(id, value, version));
        }

        public static async Task<DataObject> GetObject(this IStorageEngine storageEngine, ObjectId id)
        {
            return (await storageEngine.GetObjects(new[] { id }))[0];
        }
    }
}
