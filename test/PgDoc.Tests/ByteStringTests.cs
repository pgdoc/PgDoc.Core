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
using Xunit;

namespace PgDoc.Tests
{
    public class ByteStringTests
    {
        [Fact]
        public void Constructor_Success()
        {
            byte[] sourceArray = new byte[] { 18, 178, 255, 70, 0 };
            ByteString result = new ByteString(sourceArray);
            sourceArray[4] = 1;

            Assert.NotSame(sourceArray, result.Value.ToArray());
            Assert.Equal<byte>(new byte[] { 18, 178, 255, 70, 0 }, result.Value.ToArray());
        }

        [Fact]
        public void Constructor_Default()
        {
            Assert.Equal<byte>(new byte[0], default(ByteString).Value.ToArray());
            Assert.Equal<byte>(new byte[0], new ByteString(null).Value.ToArray());
            Assert.Equal<byte>(new byte[0], ByteString.Empty.Value.ToArray());
        }

        [Fact]
        public void Parse_Success()
        {
            ByteString result = ByteString.Parse("12b2FE460035789ACd");

            Assert.Equal<byte>(new byte[] { 18, 178, 254, 70, 0, 53, 120, 154, 205 }, result.Value.ToArray());
        }

        [Fact]
        public void Parse_InvalidLength()
        {
            Assert.Throws<FormatException>(
                () => ByteString.Parse("12b2ff460"));
        }

        [Fact]
        public void Parse_InvalidCharacter()
        {
            Assert.Throws<FormatException>(
                () => ByteString.Parse("1G"));

            Assert.Throws<FormatException>(
                () => ByteString.Parse("1/"));
        }

        [Fact]
        public void Parse_Null()
        {
            Assert.Throws<FormatException>(
                () => ByteString.Parse(null));
        }

        [Fact]
        public void ToByteArray_Success()
        {
            byte[] sourceArray = new byte[] { 18, 178, 255, 70, 0 };
            ByteString result = new ByteString(sourceArray);

            Assert.Equal<byte>(new byte[] { 18, 178, 255, 70, 0 }, result.ToByteArray());
        }

        [Fact]
        public void ToByteArray_Default()
        {
            Assert.Equal<byte>(new byte[0], default(ByteString).ToByteArray());
        }

        [Fact]
        public void ToString_Success()
        {
            string result = new ByteString(new byte[] { 18, 178, 255, 70, 0 }).ToString();

            Assert.Equal("12b2ff4600", result);
        }

        [Fact]
        public void ToString_Default()
        {
            string result = default(ByteString).ToString();

            Assert.Equal("", result);
        }

        [Theory]
        [MemberData(nameof(EqualsData))]
        public void Equals_Success(bool equal, ByteString left, ByteString right)
        {
            Assert.Equal(equal, left.Equals(right));
            Assert.Equal(equal, left == right);
            Assert.Equal(!equal, left != right);
        }

        public static IEnumerable<object[]> EqualsData => new List<object[]>()
        {
            new object[] { true, ByteString.Parse("abcd"), ByteString.Parse("abcd") },
            new object[] { false, ByteString.Parse("abcd"), ByteString.Parse("abce") },
            new object[] { false, ByteString.Parse("abcd"), ByteString.Parse("abcdef") },
            new object[] { false, ByteString.Parse("abcdef"), ByteString.Parse("abcd") },
            new object[] { true, ByteString.Empty, default(ByteString) },
            new object[] { true, default(ByteString), ByteString.Empty }
        };

        [Fact]
        public void Equals_ObjectComparison()
        {
            Assert.False(ByteString.Parse("abcd").Equals(null));
            Assert.True(ByteString.Parse("abcd").Equals((object)ByteString.Parse("abcd")));
            Assert.False(ByteString.Parse("abcd").Equals(4));
        }

        [Fact]
        public void GetHashCode_Success()
        {
            ByteString value1 = ByteString.Parse("000001");
            ByteString value2 = ByteString.Parse("000002");
            ByteString value3 = ByteString.Parse("000001");

            Assert.Equal(value1.GetHashCode(), value3.GetHashCode());
            Assert.NotEqual(value1.GetHashCode(), value2.GetHashCode());
        }

        [Fact]
        public void GetHashCode_Default()
        {
            int result = default(ByteString).GetHashCode();

            Assert.Equal(ByteString.Empty.GetHashCode(), result);
        }
    }
}
