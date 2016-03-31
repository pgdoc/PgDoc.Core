using System.Collections.Generic;
using System.Threading.Tasks;

namespace Wistap
{
    public static class StorageEngineExtensions
    {
        public static Task<ByteString> UpdateDocuments(this IStorageEngine storageEngine, params Document[] objects)
        {
            return storageEngine.UpdateDocuments((IEnumerable<Document>)objects, (IEnumerable<Document>)new Document[0]);
        }

        public static Task<ByteString> UpdateDocument(this IStorageEngine storageEngine, DocumentId id, string value, ByteString version)
        {
            return storageEngine.UpdateDocuments(new Document(id, value, version));
        }

        public static async Task<Document> GetDocument(this IStorageEngine storageEngine, DocumentId id)
        {
            return (await storageEngine.GetDocuments(new[] { id }))[0];
        }
    }
}
