using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Npgsql;
using NpgsqlTypes;
using System.Linq;

namespace Wistap
{
    public class StorageEngine : IStorageEngine
    {
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

        public async Task<ByteString> UpdateObject(ByteString id, ByteString account, string payload, ByteString version)
        {
            const string queryText =
                @"SELECT wistap.update_object(@id, @account, @payload, @version) AS result;";

            using (NpgsqlCommand command = new NpgsqlCommand(queryText, connection, this.transaction))
            {
                command.Parameters.AddWithValue("@id", NpgsqlDbType.Uuid, new Guid(id.ToByteArray()));
                command.Parameters.Add(new NpgsqlParameter("@account", account.ToByteArray()));
                command.Parameters.AddWithValue("@payload", NpgsqlDbType.Jsonb, payload != null ? (object)JObject.Parse(payload) : DBNull.Value);
                command.Parameters.Add(new NpgsqlParameter("@version", version.ToByteArray()));

                IReadOnlyList<ByteString> newVersion = await ExecuteQuery(command, reader => reader["result"] is DBNull ? null : new ByteString((byte[])reader["result"]));

                if (newVersion[0] == null)
                    throw new UpdateConflictException(id, version);
                else
                    return newVersion[0];
            }
        }

        public async Task<IReadOnlyList<DataObject>> GetObjects(ByteString account, IEnumerable<ByteString> ids)
        {
            const string queryText =
                @"SELECT id, payload, version FROM wistap.get_objects(@account, @ids);";

            List<Guid> idList = ids.Select(id => new Guid(id.ToByteArray())).ToList();

            if (idList.Count == 0)
                return new DataObject[0];

            using (NpgsqlCommand command = new NpgsqlCommand(queryText, connection, this.transaction))
            {
                command.Parameters.Add(new NpgsqlParameter("@account", account.ToByteArray()));
                command.Parameters.AddWithValue("@ids", NpgsqlDbType.Array | NpgsqlDbType.Uuid, idList);

                IReadOnlyList<DataObject> queryResult = await ExecuteQuery(
                    command,
                    reader => new DataObject(
                        new ByteString(((Guid)reader["id"]).ToByteArray()),
                        reader["payload"] is DBNull ? null : (string)reader["payload"],
                        new ByteString((byte[])reader["version"])));

                Dictionary<ByteString, DataObject> result = queryResult.ToDictionary(dataObject => dataObject.Id);

                foreach (Guid id in idList)
                {
                    ByteString byteStringId = new ByteString(id.ToByteArray());
                    if (!result.ContainsKey(byteStringId))
                        result.Add(byteStringId, new DataObject(byteStringId, null, ByteString.Empty));
                }

                return result.Values.ToList().AsReadOnly();
            }
        }

        //public async Task EnsureObject(long ids, ByteString version)
        //{
        //    const string queryText =
        //        @"SELECT id, version FROM wistap.ensure_objects(@id);";

        //    using (NpgsqlCommand command = new NpgsqlCommand(queryText, connection, this.transaction))
        //    {
        //        command.Parameters.AddWithValue("@ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint, new[] { ids });

        //        IReadOnlyList<Tuple<long, ByteString>> records = await ExecuteQuery(
        //            command,
        //            reader => new DataObject((long)reader["id"], (DataObjectType)(short)reader["type"], (string)reader["payload"], new ByteString((byte[])reader["version"])));
        //    }
        //}

        public DbTransaction StartTransaction()
        {
            this.transaction = connection.BeginTransaction(IsolationLevel.RepeatableRead);
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
