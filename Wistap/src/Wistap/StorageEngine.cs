using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Npgsql;
using NpgsqlTypes;

namespace Wistap
{
    public class StorageEngine : IStorageEngine
    {
        private readonly NpgsqlConnection connection;

        public StorageEngine(NpgsqlConnection connection)
        {
            this.connection = connection;
        }

        public async Task Initialize()
        {
            if (connection.State == ConnectionState.Closed)
                await connection.OpenAsync();
        }

        public async Task<long> CreateObject(ByteString account, DataObjectType type, string payload)
        {
            const string queryText =
                @"SELECT wistap.create_object(@account_key, @type, @payload) AS result;";

            using (NpgsqlCommand command = new NpgsqlCommand(queryText, connection))
            {
                command.Parameters.Add(new NpgsqlParameter("@account_key", account.ToByteArray()));
                command.Parameters.Add(new NpgsqlParameter("@type", (short)type));
                command.Parameters.AddWithValue("@payload", NpgsqlDbType.Jsonb, JObject.Parse(payload));

                IReadOnlyList<long> result = await ExecuteQuery(command, reader => (long)reader["result"]);

                return result[0];
            }
        }

        public async Task<ByteString> UpdateObject(long id, string payload, ByteString version)
        {
            const string queryText =
                @"SELECT wistap.update_object(@id, @payload, @version) AS result;";

            using (NpgsqlCommand command = new NpgsqlCommand(queryText, connection))
            {
                command.Parameters.Add(new NpgsqlParameter("@id", id));
                command.Parameters.AddWithValue("@payload", NpgsqlDbType.Jsonb, JObject.Parse(payload));
                command.Parameters.Add(new NpgsqlParameter("@version", version.ToByteArray()));

                IReadOnlyList<ByteString> newVersion = await ExecuteQuery(command, reader => reader["result"] is DBNull ? null : new ByteString((byte[])reader["result"]));

                if (newVersion[0] == null)
                    throw new UpdateConflictException(id, version);
                else
                    return newVersion[0];
            }
        }

        public async Task DeleteObject(long id, ByteString version)
        {
            const string queryText =
                @"SELECT wistap.delete_object(@id, @version) AS result;";

            using (NpgsqlCommand command = new NpgsqlCommand(queryText, connection))
            {
                command.Parameters.Add(new NpgsqlParameter("@id", id));
                command.Parameters.Add(new NpgsqlParameter("@version", version.ToByteArray()));

                IReadOnlyList<bool> notFound = await ExecuteQuery(command, reader => reader["result"] is DBNull);

                if (notFound[0])
                    throw new UpdateConflictException(id, version);
            }
        }

        public async Task<IReadOnlyList<DataObject>> GetObjects(ByteString account, IEnumerable<long> ids)
        {
            const string queryText =
                @"SELECT id, type, payload, version FROM wistap.get_objects(@account_key, @ids);";

            List<long> idList = new List<long>(ids);

            if (idList.Count == 0)
                return new DataObject[0];

            using (NpgsqlCommand command = new NpgsqlCommand(queryText, connection))
            {
                command.Parameters.Add(new NpgsqlParameter("@account_key", account.ToByteArray()));
                command.Parameters.AddWithValue("@ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint, idList);

                return await ExecuteQuery(
                    command,
                    reader => new DataObject((long)reader["id"], (DataObjectType)(short)reader["type"], (string)reader["payload"], new ByteString((byte[])reader["version"])));
            }
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
