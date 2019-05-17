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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace PgDoc
{
    /// <summary>
    /// Represents an immutable string of binary data.
    /// </summary>
    public class ByteString : IEquatable<ByteString>
    {
        static ByteString()
        {
            Empty = new ByteString(new byte[0]);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ByteString"/> class from a collection of bytes.
        /// </summary>
        /// <param name="data">An enumeration of bytes used to initialize the instance.</param>
        public ByteString(IEnumerable<byte> data)
        {
            Value = new ReadOnlyMemory<byte>(data.ToArray());
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ByteString"/> class from a byte array.
        /// </summary>
        /// <param name="data">The byte array used to initialize the instance</param>
        public ByteString(byte[] data)
        {
            Span<byte> span = new Span<byte>(data);
            Value = new ReadOnlyMemory<byte>(span.ToArray());
        }

        /// <summary>
        /// Gets an empty <see cref="ByteString"/>.
        /// </summary>
        public static ByteString Empty { get; }

        /// <summary>
        /// Gets a read-only buffer containing all the bytes in the <see cref="ByteString"/>.
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
        /// Returns a copy of the instance as an array.
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
            if (other == null)
            {
                return false;
            }
            else
            {
                if (Value.Length != other.Value.Length)
                    return false;

                for (int i = 0; i < other.Value.Length; i++)
                    if (Value.Span[i] != other.Value.Span[i])
                        return false;

                return true;
            }
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
            unchecked
            {
                int result = 113327;
                for (int i = 0; i < Value.Length; i++)
                    result = (result * 486187739) ^ Value.Span[i];

                return result;
            }
        }

        /// <summary>
        /// Returns the hexadecimal representation of the current object.
        /// </summary>
        /// <returns>The hexadecimal representation of the current object.</returns>
        public override string ToString()
        {
            StringBuilder hex = new StringBuilder(Value.Length * 2);

            for (int i = 0; i < Value.Length; i++)
                hex.AppendFormat("{0:x2}", Value.Span[i]);

            return hex.ToString();
        }
    }
}
