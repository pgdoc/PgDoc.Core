using System.Collections.Generic;
using System.Threading.Tasks;

namespace Wistap
{
    public static class StorageEngineExtensions
    {
        public static Task<ByteString> UpdateDocuments(this IDocumentStore documentStore, params Document[] objects)
        {
            return documentStore.UpdateDocuments((IEnumerable<Document>)objects, (IEnumerable<Document>)new Document[0]);
        }

        public static Task<ByteString> UpdateDocument(this IDocumentStore documentStore, DocumentId id, string content, ByteString version)
        {
            return documentStore.UpdateDocuments(new Document(id, content, version));
        }

        public static async Task<Document> GetDocument(this IDocumentStore documentStore, DocumentId id)
        {
            return (await documentStore.GetDocuments(new[] { id }))[0];
        }
    }
}
