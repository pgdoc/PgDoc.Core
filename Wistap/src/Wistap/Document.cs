using System;

namespace Wistap
{
    public class Document
    {
        public Document(DocumentId id, string body, ByteString version)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            if (version == null)
                throw new ArgumentNullException(nameof(version));

            this.Id = id;
            this.Body = body;
            this.Version = version;
        }

        public DocumentId Id { get; }

        public string Body { get; }

        public ByteString Version { get; }
    }
}
