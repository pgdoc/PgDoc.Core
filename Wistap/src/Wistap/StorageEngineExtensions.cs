using System.Collections.Generic;
using System.Threading.Tasks;

namespace Wistap
{
    public static class StorageEngineExtensions
    {
        public static Task<ByteString> UpdateObjects(this IStorageEngine storageEngine, params Document[] objects)
        {
            return storageEngine.UpdateObjects((IEnumerable<Document>)objects, (IEnumerable<Document>)new Document[0]);
        }

        public static Task<ByteString> UpdateObject(this IStorageEngine storageEngine, DocumentId id, string value, ByteString version)
        {
            return storageEngine.UpdateObjects(new Document(id, value, version));
        }

        public static async Task<Document> GetObject(this IStorageEngine storageEngine, DocumentId id)
        {
            return (await storageEngine.GetObjects(new[] { id }))[0];
        }
    }
}
