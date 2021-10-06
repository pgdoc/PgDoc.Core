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
using System.Threading.Tasks;

namespace PgDoc
{
    public static class DocumentStoreExtensions
    {
        /// <summary>
        /// Updates atomically the body of multiple documents.
        /// </summary>
        public static Task<ByteString> UpdateDocuments(this IDocumentStore documentStore, params Document[] objects)
        {
            return documentStore.UpdateDocuments(objects, new Document[0]);
        }

        /// <summary>
        /// Retrieves a document given its ID.
        /// </summary>
        public static async Task<Document> GetDocument(this IDocumentStore documentStore, Guid id)
        {
            return (await documentStore.GetDocuments(new[] { id }))[0];
        }
    }
}
