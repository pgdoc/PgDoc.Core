using System;

namespace Wistap
{
    public class ObjectId : IEquatable<ObjectId>
    {
        public ObjectId(Guid id)
        {
            this.Value = id;
            byte[] byteArray = id.ToByteArray();
            this.Type = (DataObjectType)(byteArray[1] * 256 + byteArray[0]);
        }

        public Guid Value { get; }

        public DataObjectType Type { get; }

        public static ObjectId New(short type)
        {
            return null;
        }

        public bool Equals(ObjectId other)
        {
            return other != null && this.Value.Equals(other.Value);
        }

        public override bool Equals(object obj)
        {
            return this.Equals(obj as ObjectId);
        }

        public override int GetHashCode()
        {
            return this.Value.GetHashCode();
        }

        public override string ToString()
        {
            return this.Value.ToString();
        }
    }
}
