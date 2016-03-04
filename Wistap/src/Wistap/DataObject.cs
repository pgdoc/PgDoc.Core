using System;

namespace Wistap
{
    public class DataObject
    {
        public DataObject(long id, DataObjectType type, string payload, ByteString version)
        {
            if (payload == null)
                throw new ArgumentNullException(nameof(payload));

            if (version == null)
                throw new ArgumentNullException(nameof(version));

            this.Id = id;
            this.Type = type;
            this.Payload = payload;
            this.Version = version;
        }

        public long Id { get; }

        public DataObjectType Type { get; }

        public string Payload { get; }

        public ByteString Version { get; }
    }
}
