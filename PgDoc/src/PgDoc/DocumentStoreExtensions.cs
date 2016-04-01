using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace PgDoc
{
    public static class DocumentStoreExtensions
    {
        public static Task<ByteString> UpdateDocuments(this IDocumentStore documentStore, params Document[] objects)
        {
            return documentStore.UpdateDocuments((IEnumerable<Document>)objects, (IEnumerable<Document>)new Document[0]);
        }

        public static Task<ByteString> UpdateDocument(this IDocumentStore documentStore, Guid id, string body, ByteString version)
        {
            return documentStore.UpdateDocuments(new Document(id, body, version));
        }

        public static async Task<Document> GetDocument(this IDocumentStore documentStore, Guid id)
        {
            return (await documentStore.GetDocuments(new[] { id }))[0];
        }
    }
}
