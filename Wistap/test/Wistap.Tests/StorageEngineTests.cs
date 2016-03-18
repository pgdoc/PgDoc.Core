using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Npgsql;
using Xunit;

namespace Wistap.Tests
{
    public class StorageEngineTests : IDisposable
    {
        private static readonly ByteString[] accounts =
            Enumerable.Range(0, 32).Select(index => new ByteString(Enumerable.Range(0, 32).Select(i => (byte)(index + 32)))).ToArray();
        private static readonly ByteString[] versions =
            Enumerable.Range(0, 32).Select(index => new ByteString(Enumerable.Range(0, 32).Select(i => (byte)index))).ToArray();

        private readonly IStorageEngine storage;

        public StorageEngineTests()
        {
            NpgsqlConnection connection = new NpgsqlConnection("Server=127.0.0.1;Port=5432;Database=wistap;User Id=postgres;Password=admin;");

            this.storage = new StorageEngine(connection);
            this.storage.Initialize().Wait();

            NpgsqlCommand command = connection.CreateCommand();
            command.CommandText = @"TRUNCATE TABLE wistap.object;";
            command.ExecuteNonQuery();
        }

        [Fact]
        public async Task CreateObject_Success()
        {
            long id = await this.storage.CreateObject(accounts[0], DataObjectType.Transaction, "{'abc':'def'}");

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(accounts[0], new[] { id });

            Assert.Equal(1, objects.Count);
            AssertObject(objects[0], id, DataObjectType.Transaction, "{'abc':'def'}");
        }

        [Fact]
        public async Task UpdateObject_Success()
        {
            long id = await this.storage.CreateObject(accounts[0], DataObjectType.Transaction, "{'abc':'def'}");
            IReadOnlyList<DataObject> initialObject = await this.storage.GetObjects(accounts[0], new[] { id });

            ByteString version = await this.storage.UpdateObject(id, "{'ghi':'jkl'}", initialObject[0].Version);

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(accounts[0], new[] { id });

            Assert.Equal(1, objects.Count);
            AssertObject(objects[0], id, DataObjectType.Transaction, "{'ghi':'jkl'}");
            Assert.NotEqual(initialObject[0].Version, objects[0].Version);
            Assert.NotEqual(version, objects[0].Version);
        }

        [Fact]
        public async Task UpdateObject_Error()
        {
            long id = await this.storage.CreateObject(accounts[0], DataObjectType.Transaction, "{'abc':'def'}");
            IReadOnlyList<DataObject> initialObject = await this.storage.GetObjects(accounts[0], new[] { id });

            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                this.storage.UpdateObject(id, "{'ghi':'jkl'}", versions[0]));

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(accounts[0], new[] { id });

            Assert.Equal(1, objects.Count);
            AssertObject(objects[0], id, DataObjectType.Transaction, "{'abc':'def'}");
            Assert.Equal(initialObject[0].Version, objects[0].Version);
            Assert.Equal(id, exception.Id);
            Assert.Equal(versions[0], exception.Version);
        }

        [Fact]
        public async Task DeleteObject_Success()
        {
            long id = await this.storage.CreateObject(accounts[0], DataObjectType.Transaction, "{'abc':'def'}");
            IReadOnlyList<DataObject> initialObject = await this.storage.GetObjects(accounts[0], new[] { id });

            await this.storage.DeleteObject(id, initialObject[0].Version);

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(accounts[0], new[] { id });

            Assert.Equal(0, objects.Count);
        }

        [Fact]
        public async Task DeleteObject_Error()
        {
            long id = await this.storage.CreateObject(accounts[0], DataObjectType.Transaction, "{'abc':'def'}");
            IReadOnlyList<DataObject> initialObject = await this.storage.GetObjects(accounts[0], new[] { id });

            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                this.storage.DeleteObject(id, versions[0]));

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(accounts[0], new[] { id });

            Assert.Equal(1, objects.Count);
            AssertObject(objects[0], id, DataObjectType.Transaction, "{'abc':'def'}");
            Assert.Equal(initialObject[0].Version, objects[0].Version);
            Assert.Equal(id, exception.Id);
            Assert.Equal(versions[0], exception.Version);
        }

        public void Dispose()
        {
            this.storage.Dispose();
        }

        #region Helper Methods

        private static void AssertObject(DataObject dataObject, long id, DataObjectType type, string payload)
        {
            Assert.Equal(id, dataObject.Id);
            Assert.Equal(DataObjectType.Transaction, dataObject.Type);
            Assert.Equal(JObject.Parse(payload), JObject.Parse(dataObject.Payload));
        }

        #endregion
    }
}
