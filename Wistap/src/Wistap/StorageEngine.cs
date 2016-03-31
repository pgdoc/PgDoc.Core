using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Npgsql;
using NpgsqlTypes;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace Wistap
{
    public class StorageEngine : IStorageEngine
    {
        private const string LockConflictSqlState = "40001";

        private readonly NpgsqlConnection connection;
        private NpgsqlTransaction transaction = null;

        public StorageEngine(NpgsqlConnection connection)
        {
            this.connection = connection;
        }

        public async Task Initialize()
        {
            if (connection.State == ConnectionState.Closed)
                await connection.OpenAsync();
        }

        public async Task<ByteString> UpdateDocuments(IEnumerable<Document> updatedDocuments, IEnumerable<Document> checkedDocuments)
        {
            IList<Tuple<Document, bool>> documents = updatedDocuments
                .Select(item => Tuple.Create(item, false))
                .Concat(checkedDocuments.Select(item => Tuple.Create(item, true))).ToList();

            JArray jsonDocuments = new JArray(documents.Select(item => JObject.FromObject(new
            {
                i = item.Item1.Id.ToString(),
                c = item.Item1.Content == null || item.Item2 ? null : JToken.Parse(item.Item1.Content).ToString(),
                v = item.Item1.Version.ToString(),
                r = item.Item2 ? 1 : 0
            })).ToArray());

            byte[] newVersion = new byte[8];

            using (MD5 md5 = MD5.Create())
            {
                byte[] md5Hash = md5.ComputeHash(Encoding.UTF8.GetBytes(jsonDocuments.ToString(Newtonsoft.Json.Formatting.None)));
                for (int i = 0; i < 8; i++)
                    newVersion[i] = md5Hash[i];
            }

            using (NpgsqlCommand command = new NpgsqlCommand("wistap.update_documents", connection, this.transaction))
            {
                command.CommandType = CommandType.StoredProcedure;
                command.Parameters.AddWithValue("@documents", NpgsqlDbType.Jsonb, jsonDocuments);
                command.Parameters.Add(new NpgsqlParameter("@version", newVersion));

                try
                {
                    await ExecuteQuery(command, reader => 0);

                    return new ByteString(newVersion);
                }
                catch (NpgsqlException exception)
                when (exception.Code == LockConflictSqlState)
                {
                    throw new UpdateConflictException(documents[0].Item1.Id, documents[0].Item1.Version);
                }
                catch (NpgsqlException exception)
                when (exception.MessageText == "check_violation" && exception.Hint == "update_documents_conflict")
                {
                    Document conflict = documents.First(item => item.Item1.Id.Value.Equals(Guid.Parse(exception.Detail))).Item1;
                    throw new UpdateConflictException(conflict.Id, conflict.Version);
                }
            }
        }

        public async Task<IReadOnlyList<Document>> GetDocuments(IEnumerable<DocumentId> ids)
        {
            List<Guid> idList = ids.Select(id => id.Value).ToList();

            if (idList.Count == 0)
                return new Document[0];

            using (NpgsqlCommand command = new NpgsqlCommand("wistap.get_documents", connection, this.transaction))
            {
                command.CommandType = CommandType.StoredProcedure;
                command.Parameters.AddWithValue("@ids", NpgsqlDbType.Array | NpgsqlDbType.Uuid, idList);

                IReadOnlyList<Document> queryResult = await ExecuteQuery(
                command,
                reader => new Document(
                    new DocumentId((Guid)reader["id"]),
                    reader["content"] is DBNull ? null : (string)reader["content"],
                    new ByteString((byte[])reader["version"])));

                Dictionary<DocumentId, Document> result = queryResult.ToDictionary(document => document.Id);

                foreach (Guid id in idList)
                {
                    DocumentId documentId = new DocumentId(id);
                    if (!result.ContainsKey(documentId))
                        result.Add(documentId, new Document(documentId, null, ByteString.Empty));
                }

                return result.Values.ToList().AsReadOnly();
            }
        }

        public DbTransaction StartTransaction()
        {
            this.transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);
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
    }
}
