using System;

namespace Wistap
{
    public class Document
    {
        public Document(DocumentId id, string content, ByteString version)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            if (version == null)
                throw new ArgumentNullException(nameof(version));

            this.Id = id;
            this.Content = content;
            this.Version = version;
        }

        public DocumentId Id { get; }

        public string Content { get; }

        public ByteString Version { get; }
    }
}
