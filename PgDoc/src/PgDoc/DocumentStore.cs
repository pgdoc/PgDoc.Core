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

namespace PgDoc
{
    public class DocumentStore : IDocumentStore
    {
        private const string LockConflictSqlState = "40001";

        private readonly NpgsqlConnection connection;
        private NpgsqlTransaction transaction = null;

        public DocumentStore(NpgsqlConnection connection)
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
                b = item.Item1.Body == null || item.Item2 ? null : JToken.Parse(item.Item1.Body).ToString(),
                v = item.Item1.Version.ToString(),
                c = item.Item2 ? 1 : 0
            })).ToArray());

            byte[] newVersion = new byte[8];

            using (MD5 md5 = MD5.Create())
            {
                byte[] md5Hash = md5.ComputeHash(Encoding.UTF8.GetBytes(jsonDocuments.ToString(Newtonsoft.Json.Formatting.None)));
                for (int i = 0; i < 8; i++)
                    newVersion[i] = md5Hash[i];
            }

            using (NpgsqlCommand command = new NpgsqlCommand("update_documents", connection, this.transaction))
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
