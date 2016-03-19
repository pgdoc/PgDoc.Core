﻿using System;
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
        private static readonly ByteString[] ids =
            Enumerable.Range(0, 32).Select(index => new ByteString(Enumerable.Range(0, 16).Select(i => (byte)index))).ToArray();

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
        public async Task UpdateObject_EmptyToValue()
        {
            ByteString version = await UpdateObject("{'abc':'def'}", ByteString.Empty);

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(account, new[] { ids[0] });

            Assert.Equal(1, objects.Count);
            AssertObject(objects[0], ids[0], "{'abc':'def'}", version);
            Assert.Equal(8, version.Value.Count);
        }

        [Fact]
        public async Task UpdateObject_EmptyToNull()
        {
            ByteString version = await UpdateObject(null, ByteString.Empty);

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(account, new[] { ids[0] });

            Assert.Equal(1, objects.Count);
            AssertObject(objects[0], ids[0], null, version);
            Assert.Equal(8, version.Value.Count);
        }

        [Fact]
        public async Task UpdateObject_ValueToValue()
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
        public async Task UpdateObject_NullToValue()
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
        public async Task UpdateObject_NullToNull()
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
        public async Task UpdateObject_ConflictObjectDoesNotExist()
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
        public async Task UpdateObject_ConflictWrongVersion()
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
        public async Task UpdateObject_ConflictObjectAlreadyExists()
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
        public async Task UpdateObject_ConflictRepeatableRead()
        {
            ByteString version1 = await UpdateObject("{'abc':'def'}", ByteString.Empty);
            ByteString version2;

            using (DbTransaction transaction = this.storage.StartTransaction())
            {
                // Start transaction 1
                await this.storage.GetObjects(account, new[] { ids[1] });

                // Update the object with transaction 2
                version2 = await (await CreateStorageEngine()).UpdateObject(ids[0], account, "{'ghi':'jkl'}", version1);

                // Try to update the object with transaction 1
                UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                    UpdateObject("{'mno':'pqr'}", version1));
            }

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(account, new[] { ids[0] });

            Assert.Equal(1, objects.Count);
            AssertObject(objects[0], ids[0], "{'ghi':'jkl'}", version2);
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
        public async Task GetRecords_NoObject()
        {
            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(account, new ByteString[0]);

            Assert.Equal(0, objects.Count);
        }

        [Fact]
        public async Task StartTransaction_Atomicity()
        {
            ByteString version1 = await UpdateObject("{'abc':'def'}", ByteString.Empty);

            using (this.storage.StartTransaction())
            {
                await this.storage.UpdateObject(ids[1], account, "{'ghi':'jkl'}", ByteString.Empty);

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

        //[Fact]
        //public async Task DeleteObject_Success()
        //{
        //    long id = await this.storage.CreateObject(accounts[0], DataObjectType.Transaction, "{'abc':'def'}");
        //    IReadOnlyList<DataObject> initialObject = await this.storage.GetObjects(accounts[0], new[] { id });

        //    await this.storage.DeleteObject(id, initialObject[0].Version);

        //    IReadOnlyList<DataObject> objects = await this.storage.GetObjects(accounts[0], new[] { id });

        //    Assert.Equal(0, objects.Count);
        //}

        //[Fact]
        //public async Task DeleteObject_VersionConflict()
        //{
        //    long id = await this.storage.CreateObject(accounts[0], DataObjectType.Transaction, "{'abc':'def'}");
        //    IReadOnlyList<DataObject> initialObject = await this.storage.GetObjects(accounts[0], new[] { id });

        //    UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
        //        this.storage.DeleteObject(id, versions[0]));

        //    IReadOnlyList<DataObject> objects = await this.storage.GetObjects(accounts[0], new[] { id });

        //    Assert.Equal(1, objects.Count);
        //    AssertObject(objects[0], id, DataObjectType.Transaction, "{'abc':'def'}");
        //    Assert.Equal(initialObject[0].Version, objects[0].Version);
        //    Assert.Equal(id, exception.Id);
        //    Assert.Equal(versions[0], exception.Version);
        //}

        //[Fact]
        //public async Task DeleteObject_RepeatableReadConflict()
        //{
        //    long id = await this.storage.CreateObject(accounts[0], DataObjectType.Transaction, "{'abc':'def'}");

        //    using (DbTransaction transaction = this.storage.StartTransaction())
        //    {
        //        IReadOnlyList<DataObject> initialObject = await this.storage.GetObjects(accounts[0], new[] { id });

        //        await (await CreateStorageEngine()).UpdateObject(id, "{'ghi':'jkl'}", initialObject[0].Version);

        //        UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
        //            this.storage.DeleteObject(id, initialObject[0].Version));
        //    }

        //    IReadOnlyList<DataObject> objects = await this.storage.GetObjects(accounts[0], new[] { id });

        //    Assert.Equal(1, objects.Count);
        //    AssertObject(objects[0], id, DataObjectType.Transaction, "{'ghi':'jkl'}");
        //}

        //#endregion



        //[Fact]
        //public async Task StartTransaction_Serializable()
        //{
        //    long id = await this.storage.CreateObject(accounts[0], DataObjectType.Transaction, "{'abc':'def'}");
        //    IReadOnlyList<DataObject> initialObject = await this.storage.GetObjects(accounts[0], new[] { id });

        //    using (DbTransaction transaction = ((StorageEngine)this.storage).StartTransaction())
        //    {

        //        await (await CreateStorageEngine()).UpdateObject(id, "{'ghi':'jkl'}", initialObject[0].Version);

        //        await this.storage.GetObjects(accounts[0], new[] { id });



        //        await this.storage.CreateObject(accounts[0], DataObjectType.Transaction, "{'123':'456'}");
        //        transaction.Commit();
        //    }

        //    IReadOnlyList<DataObject> objects = await this.storage.GetObjects(accounts[0], new[] { id });

        //    Assert.Equal(1, objects.Count);
        //    AssertObject(objects[0], id, DataObjectType.Transaction, "{'ghi':'jkl'}");
        //}

        //[Fact]
        //public async Task StartTransaction_Serializable2()
        //{
        //    long id = await this.storage.CreateObject(accounts[0], DataObjectType.Transaction, "{'abc':'def'}");
        //    IReadOnlyList<DataObject> initialObject = await this.storage.GetObjects(accounts[0], new[] { id });

        //    using (DbTransaction transaction = ((StorageEngine)this.storage).StartTransaction())
        //    {
        //        ByteString version = await this.storage.UpdateObject(id, "{'123':'789'}", initialObject[0].Version);

        //        StorageEngine secondaryConnection = await CreateStorageEngine();
        //        using (var secondaryTransaction = secondaryConnection.StartTransaction())
        //        {
        //            await secondaryConnection.GetObjects(accounts[0], new[] { id });
        //            await secondaryConnection.CreateObject(accounts[0], DataObjectType.Transaction, "{}");
        //            secondaryTransaction.Commit();
        //        }

        //        transaction.Commit();
        //    }

        //    IReadOnlyList<DataObject> objects = await this.storage.GetObjects(accounts[0], new[] { id });

        //    Assert.Equal(1, objects.Count);
        //    AssertObject(objects[0], id, DataObjectType.Transaction, "{'123':'789'}");
        //}


        //[Fact]
        //public async Task StartTransaction_RepeatableRead2()
        //{
        //    long id = await this.storage.CreateObject(accounts[0], DataObjectType.Transaction, "{'abc':'def'}");

        //    using (DbTransaction transaction = this.storage.StartTransaction())
        //    {
        //        IReadOnlyList<DataObject> initialObject = await this.storage.GetObjects(accounts[0], new[] { id });
        //        await this.storage.UpdateObject(id, "{}", initialObject[0].Version);

        //        await (await CreateStorageEngine()).UpdateObject(id, "{'ghi':'jkl'}", initialObject[0].Version);

        //        //UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
        //        //    this.storage.UpdateObject(id, "{'mno':'pqr'}", initialObject[0].Version));
        //        transaction.Commit();
        //    }

        //    IReadOnlyList<DataObject> objects = await this.storage.GetObjects(accounts[0], new[] { id });

        //    Assert.Equal(1, objects.Count);
        //    AssertObject(objects[0], id, DataObjectType.Transaction, "{'ghi':'jkl'}");
        //}

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
            return await this.storage.UpdateObject(ids[0], account, payload, version);
        }

        private static void AssertObject(DataObject dataObject, ByteString id, string payload, ByteString version)
        {
            Assert.Equal(id, dataObject.Id);

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
