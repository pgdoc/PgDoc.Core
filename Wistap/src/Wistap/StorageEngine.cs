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
        private const string TransactionConflictCode = "40001";
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

        public async Task<ByteString> UpdateObject(ObjectId id, ByteString account, string payload, ByteString version)
        {
            using (NpgsqlCommand command = new NpgsqlCommand("wistap.update_object", connection, this.transaction))
            {
                command.CommandType = CommandType.StoredProcedure;
                command.Parameters.AddWithValue("@id", NpgsqlDbType.Uuid, id.Value);
                command.Parameters.Add(new NpgsqlParameter("@account", account.ToByteArray()));
                command.Parameters.AddWithValue("@payload", NpgsqlDbType.Jsonb, payload != null ? (object)JObject.Parse(payload) : DBNull.Value);
                command.Parameters.Add(new NpgsqlParameter("@version", version.ToByteArray()));

                try
                {
                    IReadOnlyList<ByteString> newVersion = await ExecuteQuery(command, reader => reader[0] is DBNull ? null : new ByteString((byte[])reader[0]));

                    if (newVersion[0] == null)
                        throw new UpdateConflictException(id, version);
                    else
                        return newVersion[0];
                }
                catch (NpgsqlException exception) when (exception.Code == TransactionConflictCode)
                {
                    throw new UpdateConflictException(id, version);
                }
            }
        }

        public async Task<IReadOnlyList<DataObject>> GetObjects(ByteString account, IEnumerable<ObjectId> ids)
        {
            List<Guid> idList = ids.Select(id => id.Value).ToList();

            if (idList.Count == 0)
                return new DataObject[0];

            using (NpgsqlCommand command = new NpgsqlCommand("wistap.get_objects", connection, this.transaction))
            {
                command.CommandType = CommandType.StoredProcedure;
                command.Parameters.Add(new NpgsqlParameter("@account", account.ToByteArray()));
                command.Parameters.AddWithValue("@ids", NpgsqlDbType.Array | NpgsqlDbType.Uuid, idList);

                try
                {
                    IReadOnlyList<DataObject> queryResult = await ExecuteQuery(
                    command,
                    reader => new DataObject(
                        new ObjectId((Guid)reader["id"]),
                        reader["payload"] is DBNull ? null : (string)reader["payload"],
                        new ByteString((byte[])reader["version"])));

                    Dictionary<ObjectId, DataObject> result = queryResult.ToDictionary(dataObject => dataObject.Id);

                    foreach (Guid id in idList)
                    {
                        ObjectId objectId = new ObjectId(id);
                        if (!result.ContainsKey(objectId))
                            result.Add(objectId, new DataObject(objectId, null, ByteString.Empty));
                    }

                    return result.Values.ToList().AsReadOnly();
                }
                catch (NpgsqlException exception) when (exception.Code == TransactionConflictCode)
                {
                    throw new UpdateConflictException(new ObjectId(idList[0]), null);
                }
            }
        }

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
