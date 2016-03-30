using System;

namespace Wistap
{
    public class DataObject
    {
        public DataObject(ObjectId id, string value, ByteString version)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            if (version == null)
                throw new ArgumentNullException(nameof(version));

            this.Id = id;
            this.Value = value;
            this.Version = version;
        }

        public ObjectId Id { get; }

        public string Value { get; }

        public ByteString Version { get; }
    }
}
