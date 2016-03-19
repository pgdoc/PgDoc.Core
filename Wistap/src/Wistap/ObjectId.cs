using System;
using System.Threading;

namespace Wistap
{
    public class ObjectId : IEquatable<ObjectId>
    {
        private static ThreadLocal<Random> random = new ThreadLocal<Random>(() => new Random());

        public ObjectId(Guid id)
        {
            this.Value = id;
            byte[] byteArray = id.ToByteArray();
            this.Type = (DataObjectType)((byteArray[1] << 8) | byteArray[0]);
        }

        public Guid Value { get; }

        public DataObjectType Type { get; }

        public static ObjectId New(short type)
        {
            byte[] data = new byte[16];
            random.Value.NextBytes(data);

            data[0] = (byte)(type & 0xFF);
            data[1] = (byte)(type >> 8);

            return new ObjectId(new Guid(data));
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
