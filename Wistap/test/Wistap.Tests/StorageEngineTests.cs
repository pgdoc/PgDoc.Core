using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Npgsql;
using Xunit;

namespace Wistap.Tests
{
    public class StorageEngineTests : IDisposable
    {
        private static readonly ByteString account = new ByteString(Enumerable.Range(0, 32).Select(i => (byte)i));
        private static readonly ByteString wrongVersion = new ByteString(Enumerable.Range(0, 32).Select(i => (byte)255));
        private static readonly ObjectId[] ids =
            Enumerable.Range(0, 32).Select(index => new ObjectId(new Guid(Enumerable.Range(0, 16).Select(i => (byte)index).ToArray()))).ToArray();

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

        #region UpdateObject

        [Fact]
        public async Task UpdateObjects_EmptyToValue()
        {
            ByteString version = await UpdateObject("{'abc':'def'}", ByteString.Empty);

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(account, new[] { ids[0] });

            Assert.Equal(1, objects.Count);
            AssertObject(objects[0], ids[0], "{'abc':'def'}", version);
            Assert.Equal(8, version.Value.Count);
        }

        [Fact]
        public async Task UpdateObjects_EmptyToNull()
        {
            ByteString version = await UpdateObject(null, ByteString.Empty);

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(account, new[] { ids[0] });

            Assert.Equal(1, objects.Count);
            AssertObject(objects[0], ids[0], null, version);
            Assert.Equal(8, version.Value.Count);
        }

        [Fact]
        public async Task UpdateObjects_ValueToValue()
        {
            ByteString version1 = await UpdateObject("{'abc':'def'}", ByteString.Empty);
            ByteString version2 = await UpdateObject("{'ghi':'jkl'}", version1);

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(account, new[] { ids[0] });

            Assert.Equal(1, objects.Count);
            AssertObject(objects[0], ids[0], "{'ghi':'jkl'}", version2);
            Assert.Equal(8, version2.Value.Count);
            Assert.NotEqual(version1, version2);
        }

        [Fact]
        public async Task UpdateObjects_ValueToNull()
        {
            ByteString version1 = await UpdateObject("{'abc':'def'}", ByteString.Empty);
            ByteString version2 = await UpdateObject(null, version1);

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(account, new[] { ids[0] });

            Assert.Equal(1, objects.Count);
            AssertObject(objects[0], ids[0], null, version2);
            Assert.Equal(8, version2.Value.Count);
            Assert.NotEqual(version1, version2);
        }

        [Fact]
        public async Task UpdateObjects_NullToValue()
        {
            ByteString version1 = await UpdateObject(null, ByteString.Empty);
            ByteString version2 = await UpdateObject("{'ghi':'jkl'}", version1);

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(account, new[] { ids[0] });

            Assert.Equal(1, objects.Count);
            AssertObject(objects[0], ids[0], "{'ghi':'jkl'}", version2);
            Assert.Equal(8, version2.Value.Count);
            Assert.NotEqual(version1, version2);
        }

        [Fact]
        public async Task UpdateObjects_NullToNull()
        {
            ByteString version1 = await UpdateObject(null, ByteString.Empty);
            ByteString version2 = await UpdateObject(null, version1);

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(account, new[] { ids[0] });

            Assert.Equal(1, objects.Count);
            AssertObject(objects[0], ids[0], null, version2);
            Assert.Equal(8, version2.Value.Count);
            Assert.NotEqual(version1, version2);
        }

        [Fact]
        public async Task UpdateObjects_ConflictObjectDoesNotExist()
        {
            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                UpdateObject("{'abc':'def'}", wrongVersion));

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(account, new[] { ids[0] });

            Assert.Equal(1, objects.Count);
            AssertObject(objects[0], ids[0], null, ByteString.Empty);
            Assert.Equal(ids[0], exception.Id);
            Assert.Equal(wrongVersion, exception.Version);
        }

        [Fact]
        public async Task UpdateObjects_ConflictWrongVersion()
        {
            ByteString version1 = await UpdateObject("{'abc':'def'}", ByteString.Empty);

            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                UpdateObject("{'ghi':'jkl'}", wrongVersion));

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(account, new[] { ids[0] });

            Assert.Equal(1, objects.Count);
            AssertObject(objects[0], ids[0], "{'abc':'def'}", version1);
            Assert.Equal(ids[0], exception.Id);
            Assert.Equal(wrongVersion, exception.Version);
        }

        [Fact]
        public async Task UpdateObjects_ConflictObjectAlreadyExists()
        {
            ByteString version1 = await UpdateObject("{'abc':'def'}", ByteString.Empty);

            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                UpdateObject("{'ghi':'jkl'}", ByteString.Empty));

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(account, new[] { ids[0] });

            Assert.Equal(1, objects.Count);
            AssertObject(objects[0], ids[0], "{'abc':'def'}", version1);
            Assert.Equal(ids[0], exception.Id);
            Assert.Equal(ByteString.Empty, exception.Version);
        }

        [Fact]
        public async Task UpdateObjects_ConflictWrongAccount()
        {
            ByteString wrongAccount = new ByteString(account.ToByteArray().Reverse());
            ByteString version1 = await UpdateObject("{'abc':'def'}", ByteString.Empty);

            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                this.storage.UpdateObject(wrongAccount, ids[0], "{'ghi':'jkl'}", version1));

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(account, new[] { ids[0] });

            Assert.Equal(1, objects.Count);
            AssertObject(objects[0], ids[0], "{'abc':'def'}", version1);
            Assert.Equal(ids[0], exception.Id);
            Assert.Equal(version1, exception.Version);
        }

        [Fact]
        public async Task UpdateObjects_MultipleUpdateSuccess()
        {
            ByteString version1 = await UpdateObject("{'abc':'def'}", ByteString.Empty);

            ByteString version2 = await this.storage.UpdateObjects(account,
                new DataObject(ids[1], "{'ghi':'jkl'}", ByteString.Empty),
                new DataObject(ids[0], "{'nmo':'pqr'}", version1));

            IReadOnlyList<DataObject> object1 = await this.storage.GetObjects(account, new[] { ids[0] });
            IReadOnlyList<DataObject> object2 = await this.storage.GetObjects(account, new[] { ids[1] });

            AssertObject(object1[0], ids[0], "{'nmo':'pqr'}", version2);
            AssertObject(object2[0], ids[1], "{'ghi':'jkl'}", version2);
        }

        [Fact]
        public async Task UpdateObjects_MultipleUpdateConflict()
        {
            ByteString version1 = await UpdateObject("{'abc':'def'}", ByteString.Empty);

            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                this.storage.UpdateObjects(account,
                    new DataObject(ids[1], "{'ghi':'jkl'}", ByteString.Empty),
                    new DataObject(ids[0], "{'nmo':'pqr'}", wrongVersion)));

            IReadOnlyList<DataObject> object1 = await this.storage.GetObjects(account, new[] { ids[0] });
            IReadOnlyList<DataObject> object2 = await this.storage.GetObjects(account, new[] { ids[1] });

            AssertObject(object1[0], ids[0], "{'abc':'def'}", version1);
            AssertObject(object2[0], ids[1], null, ByteString.Empty);
            Assert.Equal(ids[0], exception.Id);
            Assert.Equal(wrongVersion, exception.Version);
        }

        #endregion

        #region GetObjects

        [Fact]
        public async Task GetObjects_MultipleObjects()
        {
            ByteString version1 = await UpdateObject("{'abc':'def'}", ByteString.Empty);

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(account, new[] { ids[0], ids[1] });

            Assert.Equal(2, objects.Count);
            AssertObject(objects.First(dataObject => dataObject.Id.Equals(ids[0])), ids[0], "{'abc':'def'}", version1);
            AssertObject(objects.First(dataObject => dataObject.Id.Equals(ids[1])), ids[1], null, ByteString.Empty);
        }

        [Fact]
        public async Task GetObjects_NoObject()
        {
            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(account, new ObjectId[0]);

            Assert.Equal(0, objects.Count);
        }

        #endregion

        #region Concurrency

        [Fact]
        public async Task StartTransaction_Atomicity()
        {
            ByteString version1 = await UpdateObject("{'abc':'def'}", ByteString.Empty);

            using (this.storage.StartTransaction())
            {
                await this.storage.UpdateObject(account, ids[1], "{'ghi':'jkl'}", ByteString.Empty);

                UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                    UpdateObject("{'mno':'pqr'}", wrongVersion));
            }

            IReadOnlyList<DataObject> object1 = await this.storage.GetObjects(account, new[] { ids[0] });
            IReadOnlyList<DataObject> object2 = await this.storage.GetObjects(account, new[] { ids[1] });

            Assert.Equal(1, object1.Count);
            AssertObject(object1[0], ids[0], "{'abc':'def'}", version1);
            Assert.Equal(1, object2.Count);
            AssertObject(object2[0], ids[1], null, ByteString.Empty);
        }

        [Fact]
        public async Task UpdateObjects_TransactionConflict()
        {
            ByteString version1 = await UpdateObject("{'abc':'def'}", ByteString.Empty);
            ByteString version2;
            UpdateConflictException exception;

            using (DbTransaction transaction = this.storage.StartTransaction())
            {
                // Start transaction 1
                await this.storage.GetObjects(account, new[] { ids[1] });

                // Update the object with transaction 2
                version2 = await (await CreateStorageEngine()).UpdateObject(account, ids[0], "{'ghi':'jkl'}", version1);

                // Try to update the object with transaction 1
                exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                    UpdateObject("{'mno':'pqr'}", version1));
            }

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(account, new[] { ids[0] });

            Assert.Equal(1, objects.Count);
            AssertObject(objects[0], ids[0], "{'ghi':'jkl'}", version2);
            Assert.Equal(ids[0], exception.Id);
            Assert.Equal(version1, exception.Version);
        }

        [Fact]
        public async Task UpdateObjects_UnableToLock()
        {
            ByteString version1 = await UpdateObject("{'abc':'def'}", ByteString.Empty);
            UpdateConflictException exception;

            using (DbTransaction transaction = this.storage.StartTransaction())
            {
                // Lock the object with transaction 1
                await this.storage.GetObjects(account, new[] { ids[0] });

                // Try to update the object with transaction 2
                exception = await Assert.ThrowsAsync<UpdateConflictException>(async () =>
                    await (await CreateStorageEngine()).UpdateObject(account, ids[0], "{'ghi':'jkl'}", version1));
            }

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(account, new[] { ids[0] });

            Assert.Equal(1, objects.Count);
            AssertObject(objects[0], ids[0], "{'abc': 'def'}", version1);
            Assert.Equal(ids[0], exception.Id);
            Assert.Equal(version1, exception.Version);
        }

        [Fact]
        public async Task GetObjects_TransactionConflict()
        {
            ByteString version1 = await UpdateObject("{'abc':'def'}", ByteString.Empty);
            ByteString version2;
            UpdateConflictException exception;

            using (DbTransaction transaction = this.storage.StartTransaction())
            {
                // Start transaction 1
                await this.storage.GetObjects(account, new[] { ids[1] });

                // Update the object with transaction 2
                version2 = await (await CreateStorageEngine()).UpdateObject(account, ids[0], "{'ghi':'jkl'}", version1);

                // Try to read the stale modified object from transaction 1
                exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                    this.storage.GetObjects(account, new[] { ids[0] }));
            }

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(account, new[] { ids[0] });

            Assert.Equal(1, objects.Count);
            AssertObject(objects[0], ids[0], "{'ghi':'jkl'}", version2);
            Assert.Equal(ids[0], exception.Id);
            Assert.Equal(null, exception.Version);
        }

        [Fact]
        public async Task GetObjects_UnableToLock()
        {
            ByteString version1 = await UpdateObject("{'abc':'def'}", ByteString.Empty);
            ByteString version2;
            UpdateConflictException exception;

            using (DbTransaction transaction = this.storage.StartTransaction())
            {
                // Lock the object with transaction 1
                version2 = await UpdateObject("{'ghi':'jkl'}", version1);

                // Try to read the object with transaction 2
                exception = await Assert.ThrowsAsync<UpdateConflictException>(async () =>
                    await (await CreateStorageEngine()).GetObjects(account, new[] { ids[0] }));

                transaction.Commit();
            }

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(account, new[] { ids[0] });

            Assert.Equal(1, objects.Count);
            AssertObject(objects[0], ids[0], "{'ghi':'jkl'}", version2);
            Assert.Equal(ids[0], exception.Id);
            Assert.Equal(null, exception.Version);
        }

        #endregion

        public void Dispose()
        {
            this.storage.Dispose();
        }

        #region Helper Methods

        private static async Task<StorageEngine> CreateStorageEngine()
        {
            NpgsqlConnection connection = new NpgsqlConnection("Server=127.0.0.1;Port=5432;Database=wistap;User Id=postgres;Password=admin;");

            StorageEngine engine = new StorageEngine(connection);
            await engine.Initialize();

            return engine;
        }

        private async Task<ByteString> UpdateObject(string payload, ByteString version)
        {
            return await this.storage.UpdateObject(account, ids[0], payload, version);
        }

        private static void AssertObject(DataObject dataObject, ObjectId id, string payload, ByteString version)
        {
            Assert.Equal(id.Value, dataObject.Id.Value);

            if (payload == null)
            {
                Assert.Null(dataObject.Payload);
            }
            else
            {
                Assert.NotNull(dataObject.Payload);
                Assert.Equal(JObject.Parse(payload).ToString(Newtonsoft.Json.Formatting.None), JObject.Parse(dataObject.Payload).ToString(Newtonsoft.Json.Formatting.None));
            }

            Assert.Equal(version, dataObject.Version);
        }

        #endregion
    }
}
