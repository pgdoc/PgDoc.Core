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
        private const string TransactionConflictCode = "40001";
        private const string UnableToLockCode = "55P03";
        private const string InsertConflictCore = "23505";

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

        public async Task<ByteString> UpdateObjects(ByteString account, IEnumerable<DataObject> objects)
        {
            IList<DataObject> objectList = objects.ToList();
            JArray jsonObjects = new JArray(objectList.Select(item => JObject.FromObject(new
            {
                k = item.Id.ToString(),
                p = item.Payload == null ? null : JToken.Parse(item.Payload).ToString(),
                v = item.Version.ToString()
            })).ToArray());

            byte[] newVersion;

            using (MD5 md5 = MD5.Create())
            {
                byte[] md5Hash = md5.ComputeHash(Encoding.UTF8.GetBytes(jsonObjects.ToString(Newtonsoft.Json.Formatting.None)));
                newVersion = new byte[8];
                Buffer.BlockCopy(md5Hash, 0, newVersion, 0, 8);
            }

            using (NpgsqlCommand command = new NpgsqlCommand("wistap.update_objects", connection, this.transaction))
            {
                command.CommandType = CommandType.StoredProcedure;
                command.Parameters.Add(new NpgsqlParameter("@account", account.ToByteArray()));
                command.Parameters.AddWithValue("@objects", NpgsqlDbType.Jsonb, jsonObjects);
                command.Parameters.Add(new NpgsqlParameter("@version", newVersion));

                try
                {
                    IReadOnlyList<DataObject> conflicts = await ExecuteQuery(
                        command,
                        reader => objectList.First(item => item.Id.Value.Equals((Guid)reader["id"])));

                    if (conflicts.Count > 0)
                        throw new UpdateConflictException(conflicts[0].Id, conflicts[0].Version);

                    return new ByteString(newVersion);
                }
                catch (NpgsqlException exception)
                    when (exception.Code == TransactionConflictCode || exception.Code == UnableToLockCode || exception.Code == InsertConflictCore)
                {
                    throw new UpdateConflictException(objectList[0].Id, objectList[0].Version);
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
                catch (NpgsqlException exception) when (exception.Code == TransactionConflictCode || exception.Code == UnableToLockCode)
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
