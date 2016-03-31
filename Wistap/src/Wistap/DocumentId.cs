using System;
using System.Security.Cryptography;
using System.Threading;

namespace Wistap
{
    public class DocumentId : IEquatable<DocumentId>
    {
        private static readonly ThreadLocal<RandomNumberGenerator> random = new ThreadLocal<RandomNumberGenerator>(() => RandomNumberGenerator.Create());

        public DocumentId(Guid id)
        {
            this.Value = id;
            byte[] byteArray = id.ToByteArray();
            this.Type = (DataObjectType)((byteArray[3] << 8) | byteArray[2]);
        }

        public Guid Value { get; }

        public DataObjectType Type { get; }

        public static DocumentId New(short type)
        {
            byte[] data = new byte[16];
            random.Value.GetBytes(data);

            data[2] = (byte)(type & 0xFF);
            data[3] = (byte)(type >> 8);

            return new DocumentId(new Guid(data));
        }

        public bool Equals(DocumentId other)
        {
            return other != null && this.Value.Equals(other.Value);
        }

        public override bool Equals(object obj)
        {
            return this.Equals(obj as DocumentId);
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
