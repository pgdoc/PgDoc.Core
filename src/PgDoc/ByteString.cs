// Copyright 2016 Flavien Charlon
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.IO;
using System.Text;

namespace PgDoc
{
    /// <summary>
    /// Represents an immutable string of binary data.
    /// </summary>
    public readonly struct ByteString : IEquatable<ByteString>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ByteString"/> structure from a buffer of bytes.
        /// </summary>
        /// <param name="data">The buffer of bytes used to initialize the instance.</param>
        public ByteString(ReadOnlySpan<byte> data)
        {
            Value = new ReadOnlyMemory<byte>(data.ToArray());
        }

        /// <summary>
        /// Gets an empty <see cref="ByteString"/>.
        /// </summary>
        public static ByteString Empty { get; } = new ByteString(Span<byte>.Empty);

        /// <summary>
        /// Gets a read-only buffer containing all the bytes in the current object.
        /// </summary>
        public ReadOnlyMemory<byte> Value { get; }

        /// <summary>
        /// Parses a <see cref="ByteString"/> from a hexadecimal string.
        /// </summary>
        /// <param name="hexValue">The hexadecimal string to parse.</param>
        /// <returns>The parsed <see cref="ByteString"/> instance.</returns>
        public static ByteString Parse(string hexValue)
        {
            if (hexValue == null)
                throw new FormatException("The hexValue parameter must not be null.");

            if (hexValue.Length % 2 == 1)
                throw new FormatException("The hexValue parameter must have an even number of digits.");

            byte[] result = new byte[hexValue.Length >> 1];

            for (int i = 0; i < (hexValue.Length >> 1); ++i)
                result[i] = (byte)((GetHexValue(hexValue[i << 1]) << 4) + (GetHexValue(hexValue[(i << 1) + 1])));

            return new ByteString(result);
        }

        private static int GetHexValue(char hex)
        {
            int value = "0123456789ABCDEF".IndexOf(char.ToUpper(hex));

            if (value < 0)
                throw new FormatException(string.Format("The character '{0}' is not a hexadecimal digit.", hex));
            else
                return value;
        }

        /// <summary>
        /// Returns a copy of the current object as a byte array.
        /// </summary>
        /// <returns>A byte array representing this <see cref="ByteString"/> instance.</returns>
        public byte[] ToByteArray()
        {
            return Value.Span.ToArray();
        }

        /// <summary>
        /// Returns a read-only stream containing the data represented by the current object.
        /// </summary>
        /// <returns>A <see cref="Stream"/> representing this <see cref="ByteString"/> instance.</returns>
        public Stream ToStream()
        {
            return new MemoryStream(Value.ToArray(), 0, Value.Length, false);
        }

        /// <summary>
        /// Determines whether the specified object is equal to the current object.
        /// </summary>
        /// <param name="other">The object to compare with the current object.</param>
        /// <returns>true if the specified object is equal to the current object; otherwise, false.</returns>
        public bool Equals(ByteString other)
        {
            return this == other;
        }

        /// <summary>
        /// Determines whether the specified object is equal to the current object.
        /// </summary>
        /// <param name="obj">The object to compare with the current object.</param>
        /// <returns>true if the specified object is equal to the current object; otherwise, false.</returns>
        public override bool Equals(object obj)
        {
            if (obj is ByteString)
                return Equals((ByteString)obj);
            else
                return false;
        }

        /// <summary>
        /// Serves as a hash function for a particular type.
        /// </summary>
        /// <returns>A hash code for the current object.</returns>
        public override int GetHashCode()
        {
            ReadOnlySpan<byte> span = Value.Span;

            unchecked
            {
                int result = 113327;
                for (int i = 0; i < span.Length; i++)
                    result = (result * 486187739) ^ span[i];

                return result;
            }
        }

        /// <summary>
        /// Returns the hexadecimal representation of the current object.
        /// </summary>
        /// <returns>The hexadecimal representation of the current object.</returns>
        public override string ToString()
        {
            ReadOnlySpan<byte> span = Value.Span;

            StringBuilder hex = new StringBuilder(span.Length * 2);

            for (int i = 0; i < span.Length; i++)
                hex.AppendFormat("{0:x2}", span[i]);

            return hex.ToString();
        }

        /// <summary>
        /// Indicates whether the values of two specified <see cref="ByteString"/> objects are equal.
        /// </summary>
        /// <returns>true if a and b are equal; otherwise, false.</returns>
        public static bool operator ==(in ByteString a, in ByteString b)
        {
            ReadOnlySpan<byte> spanA = a.Value.Span;
            ReadOnlySpan<byte> spanB = b.Value.Span;

            if (spanA.Length != spanB.Length)
                return false;

            for (int i = 0; i < spanA.Length; i++)
                if (spanA[i] != spanB[i])
                    return false;

            return true;
        }

        /// <summary>
        /// Indicates whether the values of two specified <see cref="ByteString"/> objects are not equal.
        /// </summary>
        /// <returns>true if a and b are not equal; otherwise, false.</returns>
        public static bool operator !=(in ByteString left, in ByteString right)
        {
            return !(left == right);
        }
    }
}
