﻿// Copyright 2016 Flavien Charlon
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
using Xunit;

namespace PgDoc.Tests
{
    public class DocumentTests
    {
        private static readonly string _guid = "f81428a9-0bd9-4d75-95bf-976225f24cf1";

        [Fact]
        public void Constructor_Success()
        {
            Document document1 = new Document(Guid.Parse(_guid), "{'abc':'def'}", ByteString.Parse("abcd"));
            Document document2 = new Document(Guid.Parse(_guid), null, ByteString.Parse("abcd"));

            Assert.Equal(Guid.Parse(_guid), document1.Id);
            Assert.Equal("{'abc':'def'}", document1.Body);
            Assert.Equal(ByteString.Parse("abcd"), document1.Version);
            Assert.Equal(Guid.Parse(_guid), document2.Id);
            Assert.Null(document2.Body);
            Assert.Equal(ByteString.Parse("abcd"), document2.Version);
        }

        [Fact]
        public void Deconstruct_Success()
        {
            Document document = new Document(Guid.Parse(_guid), "{'abc':'def'}", ByteString.Parse("abcd"));

            (Guid id, string body, ByteString version) = document;

            Assert.Equal(Guid.Parse(_guid), id);
            Assert.Equal("{'abc':'def'}", body);
            Assert.Equal(ByteString.Parse("abcd"), version);
        }
    }
}
