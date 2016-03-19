using System;

namespace Wistap
{
    public class DataObject
    {
        public DataObject(ByteString id, string payload, ByteString version)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            if (version == null)
                throw new ArgumentNullException(nameof(version));

            this.Id = id;
            this.Payload = payload;
            this.Version = version;
        }

        public ByteString Id { get; }

        public string Payload { get; }

        public ByteString Version { get; }
    }
}
