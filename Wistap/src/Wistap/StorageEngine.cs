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
        private const string UpdateConflictCode = "40001";
        private const string InsertConflictCode = "23505";

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

        public async Task<ByteString> UpdateObjects(ByteString account, IEnumerable<DataObject> updateObjects, IEnumerable<DataObject> checkObjects)
        {
            IList<Tuple<DataObject, bool>> objectList = updateObjects
                .Select(item => Tuple.Create(item, false))
                .Concat(checkObjects.Select(item => Tuple.Create(item, true))).ToList();

            JArray jsonObjects = new JArray(objectList.Select(item => JObject.FromObject(new
            {
                i = item.Item1.Id.ToString(),
                p = item.Item1.Payload == null || item.Item2 ? null : JToken.Parse(item.Item1.Payload).ToString(),
                v = item.Item1.Version.ToString(),
                c = item.Item2 ? 1 : 0
            })).ToArray());

            byte[] newVersion = new byte[8];

            using (MD5 md5 = MD5.Create())
            {
                byte[] md5Hash = md5.ComputeHash(Encoding.UTF8.GetBytes(jsonObjects.ToString(Newtonsoft.Json.Formatting.None)));
                for (int i = 0; i < 8; i++)
                    newVersion[i] = md5Hash[i];
            }

            using (NpgsqlCommand command = new NpgsqlCommand("wistap.update_objects", connection, this.transaction))
            {
                command.CommandType = CommandType.StoredProcedure;
                command.Parameters.Add(new NpgsqlParameter("@account", account.ToByteArray()));
                command.Parameters.AddWithValue("@objects", NpgsqlDbType.Jsonb, jsonObjects);
                command.Parameters.Add(new NpgsqlParameter("@version", newVersion));

                try
                {
                    await ExecuteQuery(command, reader => 0);

                    return new ByteString(newVersion);
                }
                catch (NpgsqlException exception)
                when (exception.Code == UpdateConflictCode || exception.Code == InsertConflictCode)
                {
                    throw new UpdateConflictException(objectList[0].Item1.Id, objectList[0].Item1.Version);
                }
                catch (NpgsqlException exception)
                when (exception.MessageText == "check_violation" && exception.Hint == "update_objects_conflict")
                {
                    DataObject conflict = objectList.First(item => item.Item1.Id.Value.Equals(Guid.Parse(exception.Detail))).Item1;
                    throw new UpdateConflictException(conflict.Id, conflict.Version);
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
