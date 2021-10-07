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
using Npgsql;
using NpgsqlTypes;

namespace PgDoc
{
    /// <summary>
    /// Represents an implementation of the <see cref="IDocumentStore" /> interface that relies on PosgreSQL for
    /// persistence.
    /// </summary>
    public class SqlDocumentStore : IDocumentStore
    {
        private const string SerializationFailureSqlState = "40001";
        private const string DeadlockDetectedSqlState = "40P01";

        private readonly NpgsqlConnection _connection;
        private NpgsqlTransaction? _transaction = null;

        public SqlDocumentStore(NpgsqlConnection connection)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        public async Task Initialize()
        {
            if (_connection.State == ConnectionState.Closed)
            {
                await _connection.OpenAsync();
                _connection.TypeMapper.MapComposite<DocumentUpdate>("document_update");
            }
        }

        public async Task<ByteString> UpdateDocuments(IEnumerable<Document> updatedDocuments, IEnumerable<Document> checkedDocuments)
        {
            List<DocumentUpdate> documents = new List<DocumentUpdate>();

            foreach (Document document in updatedDocuments)
            {
                documents.Add(new DocumentUpdate()
                {
                    Id = document.Id,
                    Body = document.Body,
                    Version = document.Version.ToByteArray(),
                    CheckOnly = false
                });
            }

            foreach (Document document in checkedDocuments)
            {
                documents.Add(new DocumentUpdate()
                {
                    Id = document.Id,
                    Body = null,
                    Version = document.Version.ToByteArray(),
                    CheckOnly = true
                });
            }

            byte[] newVersion = new byte[16];
            using (RandomNumberGenerator rng = RandomNumberGenerator.Create())
                rng.GetBytes(newVersion);

            using NpgsqlCommand command = new NpgsqlCommand("update_documents", _connection, _transaction);

            command.CommandType = CommandType.StoredProcedure;
            command.Parameters.Add(new NpgsqlParameter()
            {
                ParameterName = "@document_updates",
                Value = documents
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
                throw new UpdateConflictException(documents[0].Id, new ByteString(documents[0].Version));
            }
            catch (PostgresException exception)
            when (exception.MessageText == "update_documents_conflict")
            {
                DocumentUpdate conflict = documents.First(item => item.Id.Equals(Guid.Parse(exception.Hint)));
                throw new UpdateConflictException(conflict.Id, new ByteString(conflict.Version));
            }
        }

        public async Task<IReadOnlyList<Document>> GetDocuments(IEnumerable<Guid> ids)
        {
            List<Guid> idList = ids.ToList();

            if (idList.Count == 0)
                return new Document[0];

            using NpgsqlCommand command = new NpgsqlCommand("get_documents", _connection, _transaction);

            command.CommandType = CommandType.StoredProcedure;
            command.Parameters.AddWithValue("@ids", NpgsqlDbType.Array | NpgsqlDbType.Uuid, idList);

            IReadOnlyList<Document> queryResult = await ExecuteQuery(
                command,
                reader => new Document(
                    (Guid)reader["id"],
                    reader["body"] is DBNull ? null : (string)reader["body"],
                    new ByteString((byte[])reader["version"])));

            Dictionary<Guid, Document> documents = queryResult.ToDictionary(document => document.Id);

            List<Document> result = new List<Document>(idList.Count);
            foreach (Guid id in idList)
            {
                if (documents.TryGetValue(id, out Document document))
                    result.Add(document);
                else
                    result.Add(new Document(id, null, ByteString.Empty));
            }

            return result.AsReadOnly();
        }

        public DbTransaction StartTransaction(IsolationLevel isolationLevel)
        {
            _transaction = _connection.BeginTransaction(isolationLevel);
            return _transaction;
        }

        public void Dispose()
        {
            _connection.Dispose();
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

        private sealed class DocumentUpdate
        {
            public Guid Id { get; set; }

            public string? Body { get; set; }

            public byte[] Version { get; set; } = new byte[0];

            public bool CheckOnly { get; set; }
        }
    }
}
