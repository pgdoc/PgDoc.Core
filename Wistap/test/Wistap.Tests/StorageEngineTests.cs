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

        private readonly StorageEngine storage;

        public StorageEngineTests()
        {
            NpgsqlConnection connection = new NpgsqlConnection("Server=127.0.0.1;Port=5432;Database=wistap;User Id=postgres;Password=admin;CommandTimeout=1");
            this.storage = new StorageEngine(connection);
            this.storage.Initialize().Wait();

            NpgsqlCommand command = connection.CreateCommand();
            command.CommandText = @"TRUNCATE TABLE wistap.object;";
            command.ExecuteNonQuery();
        }

        #region UpdateObject

        [Theory]
        [InlineData("{'abc':'def'}")]
        [InlineData(null)]
        public async Task UpdateObjects_EmptyToValue(string to)
        {
            ByteString version = await UpdateObject(to, ByteString.Empty);

            DataObject dataObject = await this.storage.GetObject(account, ids[0]);

            AssertObject(dataObject, ids[0], to, version);
            Assert.Equal(8, version.Value.Count);
        }

        [Theory]
        [InlineData("{'abc':'def'}", "{'ghi':'jkl'}")]
        [InlineData(null, "{'ghi':'jkl'}")]
        [InlineData("{'abc':'def'}", null)]
        [InlineData(null, null)]
        public async Task UpdateObjects_ValueToValue(string from, string to)
        {
            ByteString version1 = await UpdateObject(from, ByteString.Empty);
            ByteString version2 = await UpdateObject(to, version1);

            DataObject dataObject = await this.storage.GetObject(account, ids[0]);

            AssertObject(dataObject, ids[0], to, version2);
            Assert.Equal(8, version2.Value.Count);
            Assert.NotEqual(version1, version2);
        }

        [Fact]
        public async Task UpdateObjects_EmptyToCheck()
        {
            ByteString version = await CheckObject(ByteString.Empty);

            DataObject dataObject = await this.storage.GetObject(account, ids[0]);

            AssertObject(dataObject, ids[0], null, ByteString.Empty);
            Assert.Equal(8, version.Value.Count);
        }

        [Theory]
        [InlineData("{'abc':'def'}")]
        [InlineData(null)]
        public async Task UpdateObjects_ValueToCheck(string from)
        {
            ByteString version1 = await UpdateObject(from, ByteString.Empty);
            ByteString version2 = await CheckObject(version1);

            DataObject dataObject = await this.storage.GetObject(account, ids[0]);

            AssertObject(dataObject, ids[0], from, version1);
            Assert.Equal(8, version2.Value.Count);
            Assert.NotEqual(version1, version2);
        }

        [Theory]
        [InlineData("{'abc':'def'}")]
        [InlineData(null)]
        public async Task UpdateObjects_CheckToValue(string to)
        {
            await CheckObject(ByteString.Empty);
            ByteString version = await UpdateObject(to, ByteString.Empty);

            DataObject dataObject = await this.storage.GetObject(account, ids[0]);

            AssertObject(dataObject, ids[0], to, version);
            Assert.Equal(8, version.Value.Count);
        }

        [Fact]
        public async Task UpdateObjects_CheckToCheck()
        {
            await CheckObject(ByteString.Empty);
            await CheckObject(ByteString.Empty);

            DataObject dataObject = await this.storage.GetObject(account, ids[0]);

            AssertObject(dataObject, ids[0], null, ByteString.Empty);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task UpdateObjects_ConflictObjectDoesNotExist(bool checkOnly)
        {
            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                checkOnly
                ? CheckObject(wrongVersion)
                : UpdateObject("{'abc':'def'}", wrongVersion));

            DataObject dataObject = await this.storage.GetObject(account, ids[0]);

            AssertObject(dataObject, ids[0], null, ByteString.Empty);
            Assert.Equal(ids[0], exception.Id);
            Assert.Equal(wrongVersion, exception.Version);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task UpdateObjects_ConflictWrongVersion(bool checkOnly)
        {
            ByteString version1 = await UpdateObject("{'abc':'def'}", ByteString.Empty);

            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                checkOnly
                ? CheckObject(wrongVersion)
                : UpdateObject("{'ghi':'jkl'}", wrongVersion));

            DataObject dataObject = await this.storage.GetObject(account, ids[0]);

            AssertObject(dataObject, ids[0], "{'abc':'def'}", version1);
            Assert.Equal(ids[0], exception.Id);
            Assert.Equal(wrongVersion, exception.Version);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task UpdateObjects_ConflictObjectAlreadyExists(bool checkOnly)
        {
            ByteString version1 = await UpdateObject("{'abc':'def'}", ByteString.Empty);

            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                checkOnly
                ? CheckObject(ByteString.Empty)
                : UpdateObject("{'ghi':'jkl'}", ByteString.Empty));

            DataObject dataObject = await this.storage.GetObject(account, ids[0]);

            AssertObject(dataObject, ids[0], "{'abc':'def'}", version1);
            Assert.Equal(ids[0], exception.Id);
            Assert.Equal(ByteString.Empty, exception.Version);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task UpdateObjects_ConflictWrongAccount(bool checkOnly)
        {
            ByteString wrongAccount = new ByteString(account.ToByteArray().Reverse());
            ByteString version1 = await UpdateObject("{'abc':'def'}", ByteString.Empty);

            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                checkOnly
                ? this.storage.UpdateObjects(wrongAccount, new DataObject[0], new[] { new DataObject(ids[0], "{'ghi':'jkl'}", version1) })
                : this.storage.UpdateObject(wrongAccount, ids[0], "{'ghi':'jkl'}", version1));

            DataObject dataObject = await this.storage.GetObject(account, ids[0]);

            AssertObject(dataObject, ids[0], "{'abc':'def'}", version1);
            Assert.Equal(ids[0], exception.Id);
            Assert.Equal(version1, exception.Version);
        }

        [Fact]
        public async Task UpdateObjects_MultipleObjectsSuccess()
        {
            ByteString version1 = await this.storage.UpdateObject(account, ids[0], "{'abc':'def'}", ByteString.Empty);
            ByteString version2 = await this.storage.UpdateObject(account, ids[1], "{'ghi':'jkl'}", ByteString.Empty);

            ByteString version3 = await this.storage.UpdateObjects(account,
                new DataObject[]
                {
                    new DataObject(ids[0], "{'v':'1'}", version1),
                    new DataObject(ids[2], "{'v':'2'}", ByteString.Empty)
                },
                new DataObject[]
                {
                    new DataObject(ids[1], "{'v':'3'}", version2),
                    new DataObject(ids[3], "{'v':'4'}", ByteString.Empty)
                });

            DataObject object1 = await this.storage.GetObject(account, ids[0]);
            DataObject object2 = await this.storage.GetObject(account, ids[1]);
            DataObject object3 = await this.storage.GetObject(account, ids[2]);
            DataObject object4 = await this.storage.GetObject(account, ids[3]);

            AssertObject(object1, ids[0], "{'v':'1'}", version3);
            AssertObject(object2, ids[1], "{'ghi':'jkl'}", version2);
            AssertObject(object3, ids[2], "{'v':'2'}", version3);
            AssertObject(object4, ids[3], null, ByteString.Empty);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task UpdateObjects_MultipleObjectsConflict(bool checkOnly)
        {
            ByteString version1 = await this.storage.UpdateObject(account, ids[0], "{'abc':'def'}", ByteString.Empty);

            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(async delegate ()
            {
                if (checkOnly)
                    await this.storage.UpdateObjects(account,
                        new DataObject[] { new DataObject(ids[0], "{'ghi':'jkl'}", version1) },
                        new DataObject[] { new DataObject(ids[1], "{'mno':'pqr'}", wrongVersion) });
                else
                    await this.storage.UpdateObjects(account,
                        new DataObject(ids[0], "{'ghi':'jkl'}", version1),
                        new DataObject(ids[1], "{'mno':'pqr'}", wrongVersion));
            });

            DataObject object1 = await this.storage.GetObject(account, ids[0]);
            DataObject object2 = await this.storage.GetObject(account, ids[1]);

            AssertObject(object1, ids[0], "{'abc':'def'}", version1);
            AssertObject(object2, ids[1], null, ByteString.Empty);
            Assert.Equal(ids[1], exception.Id);
            Assert.Equal(wrongVersion, exception.Version);
        }

        #endregion

        #region GetObjects

        [Fact]
        public async Task GetObjects_SingleObject()
        {
            ByteString version1 = await UpdateObject("{'abc':'def'}", ByteString.Empty);

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(account, new[] { ids[0] });

            Assert.Equal(1, objects.Count);
            AssertObject(objects.First(dataObject => dataObject.Id.Equals(ids[0])), ids[0], "{'abc':'def'}", version1);
        }

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

            DataObject object1 = await this.storage.GetObject(account, ids[0]);
            DataObject object2 = await this.storage.GetObject(account, ids[1]);

            AssertObject(object1, ids[0], "{'abc':'def'}", version1);
            AssertObject(object2, ids[1], null, ByteString.Empty);
        }

        [Theory]
        [InlineData(true, false)]
        [InlineData(false, false)]
        [InlineData(true, true)]
        [InlineData(false, true)]
        public async Task UpdateObjects_SerializationError(bool checkOnly, bool isInsert)
        {
            ByteString initialVersion = isInsert ? ByteString.Empty : await UpdateObject("{'abc':'def'}", ByteString.Empty);
            ByteString updatedVersion;
            UpdateConflictException exception;

            using (DbTransaction transaction = this.storage.StartTransaction())
            {
                // Start transaction 1
                await this.storage.GetObject(account, ids[1]);

                // Update the object with transaction 2
                updatedVersion = await (await CreateStorageEngine()).UpdateObject(account, ids[0], "{'ghi':'jkl'}", initialVersion);

                // Try to update or check the version of the object with transaction 1
                exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                    checkOnly
                    ? CheckObject(initialVersion)
                    : UpdateObject("{'mno':'pqr'}", initialVersion));
            }

            DataObject dataObject = await this.storage.GetObject(account, ids[0]);

            AssertObject(dataObject, ids[0], "{'ghi':'jkl'}", updatedVersion);
            Assert.Equal(ids[0], exception.Id);
            Assert.Equal(initialVersion, exception.Version);
        }

        [Theory]
        [InlineData(true, false)]
        [InlineData(false, false)]
        [InlineData(true, true)]
        [InlineData(false, true)]
        public async Task UpdateObjects_WaitForLock(bool checkOnly, bool isInsert)
        {
            ByteString initialVersion = isInsert ? ByteString.Empty : await UpdateObject("{'abc':'def'}", ByteString.Empty);

            StorageEngine connection2 = await CreateStorageEngine();
            using (DbTransaction transaction = connection2.StartTransaction())
            {
                // Lock the object with transaction 2
                await connection2.UpdateObjects(account, new DataObject[0], new[] { new DataObject(ids[0], "{'ignored':'ignored'}", initialVersion) });

                // Try to update or check the version of the object with transaction 1
                await Assert.ThrowsAsync<TaskCanceledException>(() =>
                    checkOnly
                    ? CheckObject(initialVersion)
                    : UpdateObject("{'mno':'pqr'}", initialVersion));

                transaction.Commit();
            }

            DataObject dataObject = await this.storage.GetObject(account, ids[0]);

            AssertObject(dataObject, ids[0], isInsert ? null : "{'abc':'def'}", initialVersion);
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

        private async Task<ByteString> CheckObject(ByteString version)
        {
            return await this.storage.UpdateObjects(account, new DataObject[0], new[] { new DataObject(ids[0], "{'ignored':'ignored'}", version) });
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
