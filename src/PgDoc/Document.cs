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

namespace PgDoc
{
    /// <summary>
    /// Represents a document comprised of a unique ID, a JSON body and a version.
    /// </summary>
    public class Document
    {
        public Document(Guid id, string? body, ByteString version)
        {
            Id = id;
            Body = body;
            Version = version;
        }

        /// <summary>
        /// Gets the unique identifier of the document.
        /// </summary>
        public Guid Id { get; }

        /// <summary>
        /// Gets the JSON body of the document as a string, or null if the document does not exist.
        /// </summary>
        public string? Body { get; }

        /// <summary>
        /// Gets the current version of the document.
        /// </summary>
        public ByteString Version { get; }

        /// <summary>
        /// Deconstructs the ID, body and version of the <see cref="Document"/> object.
        /// </summary>
        public void Deconstruct(out Guid id, out string? body, out ByteString version)
        {
            id = Id;
            body = Body;
            version = Version;
        }
    }
}
