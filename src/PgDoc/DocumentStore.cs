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
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Npgsql;
using NpgsqlTypes;

namespace PgDoc
{
    public class DocumentStore : IDocumentStore
    {
        private const string SerializationFailureSqlState = "40001";
        private const string DeadlockDetectedSqlState = "40P01";

        private readonly NpgsqlConnection connection;
        private NpgsqlTransaction? transaction = null;

        public DocumentStore(NpgsqlConnection connection)
        {
            this.connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        public async Task Initialize()
        {
            if (connection.State == ConnectionState.Closed)
            {
                await connection.OpenAsync();
                connection.TypeMapper.MapComposite<DocumentUpdate>("document_update");
            }
        }

        public async Task<ByteString> UpdateDocuments(IEnumerable<Document> updatedDocuments, IEnumerable<Document> checkedDocuments)
        {
            IList<Tuple<Document, bool>> documents = updatedDocuments
                .Select(item => Tuple.Create(item, false))
                .Concat(checkedDocuments.Select(item => Tuple.Create(item, true))).ToList();

            DocumentUpdate[] updates = documents.Select(item => new DocumentUpdate()
            {
                Id = item.Item1.Id,
                Body = item.Item1.Body == null || item.Item2 ? null : JToken.Parse(item.Item1.Body).ToString(Formatting.None),
                Version = item.Item1.Version.ToByteArray(),
                CheckOnly = item.Item2
            }).ToArray();

            byte[] newVersion = new byte[16];
            using (RandomNumberGenerator rng = RandomNumberGenerator.Create())
                rng.GetBytes(newVersion);

            using (NpgsqlCommand command = new NpgsqlCommand("update_documents", connection, this.transaction))
            {
                command.CommandType = CommandType.StoredProcedure;
                command.Parameters.Add(new NpgsqlParameter()
                {
                    ParameterName = "@document_updates",
                    Value = updates
                });
                command.Parameters.Add(new NpgsqlParameter("@version", newVersion));

                try
                {
                    await ExecuteQuery(command, reader => 0);

                    return new ByteString(newVersion);
                }
                catch (PostgresException exception)
                when (exception.SqlState == SerializationFailureSqlState || exception.SqlState == DeadlockDetectedSqlState)
                {
                    throw new UpdateConflictException(documents[0].Item1.Id, documents[0].Item1.Version);
                }
                catch (PostgresException exception)
                when (exception.MessageText == "check_violation" && exception.Hint == "update_documents_conflict")
                {
                    Document conflict = documents.First(item => item.Item1.Id.Equals(Guid.Parse(exception.Detail))).Item1;
                    throw new UpdateConflictException(conflict.Id, conflict.Version);
                }
            }
        }

        public async Task<IReadOnlyList<Document>> GetDocuments(IEnumerable<Guid> ids)
        {
            List<Guid> idList = ids.ToList();

            if (idList.Count == 0)
                return new Document[0];

            using (NpgsqlCommand command = new NpgsqlCommand("get_documents", connection, this.transaction))
            {
                command.CommandType = CommandType.StoredProcedure;
                command.Parameters.AddWithValue("@ids", NpgsqlDbType.Array | NpgsqlDbType.Uuid, idList);

                IReadOnlyList<Document> queryResult = await ExecuteQuery(
                    command,
                    reader => new Document(
                        (Guid)reader["id"],
                        reader["body"] is DBNull ? null : (string)reader["body"],
                        new ByteString((byte[])reader["version"])));

                Dictionary<Guid, Document> result = queryResult.ToDictionary(document => document.Id);

                foreach (Guid id in idList)
                {
                    if (!result.ContainsKey(id))
                        result.Add(id, new Document(id, null, ByteString.Empty));
                }

                return result.Values.ToList().AsReadOnly();
            }
        }

        public DbTransaction StartTransaction(IsolationLevel isolationLevel)
        {
            this.transaction = connection.BeginTransaction(isolationLevel);
            return this.transaction;
        }

        public void Dispose()
        {
            this.connection.Dispose();
        }

        private async Task<IReadOnlyList<T>> ExecuteQuery<T>(NpgsqlCommand command, Func<DbDataReader, T> readRecord)
        {
            List<T> result = new List<T>();

            using (DbDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.Default | CommandBehavior.SingleResult))
            {
                while (await reader.ReadAsync())
                    result.Add(readRecord(reader));
            }

            return result.AsReadOnly();
        }

        private class DocumentUpdate
        {
            public Guid Id { get; set; }

            public string? Body { get; set; }

            public byte[] Version { get; set; } = new byte[0];

            public bool CheckOnly { get; set; }
        }
    }
}
