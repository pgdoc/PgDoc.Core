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

namespace PgDoc;

using System.Collections.Generic;
using System.Threading;
using Npgsql;

public interface ISqlDocumentStore : IDocumentStore
{
    /// <summary>
    /// Executes a SQL query and converts the result into an asynchronous stream of <see cref="Document"/> objects.
    /// The query must return the id, body and version columns.
    /// </summary>
    IAsyncEnumerable<Document> ExecuteQuery(NpgsqlCommand command, CancellationToken cancel = default);
}
