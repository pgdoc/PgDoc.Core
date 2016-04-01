using System;

namespace Wistap
{
    public class Document
    {
        public Document(Guid id, string body, ByteString version)
        {
            if (version == null)
                throw new ArgumentNullException(nameof(version));

            this.Id = id;
            this.Body = body;
            this.Version = version;
        }

        public Guid Id { get; }

        public string Body { get; }

        public ByteString Version { get; }
    }
}
