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

        #region Basic Operations

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
        public async Task UpdateObject_VersionConflict()
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
        public async Task UpdateObject_RepeatableReadConflict()
        {
            long id = await this.storage.CreateObject(accounts[0], DataObjectType.Transaction, "{'abc':'def'}");
            
            using (DbTransaction transaction = this.storage.StartTransaction())
            {
                IReadOnlyList<DataObject> initialObject = await this.storage.GetObjects(accounts[0], new[] { id });

                await (await CreateStorageEngine()).UpdateObject(id, "{'ghi':'jkl'}", initialObject[0].Version);

                UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                    this.storage.UpdateObject(id, "{'mno':'pqr'}", initialObject[0].Version));
            }

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(accounts[0], new[] { id });

            Assert.Equal(1, objects.Count);
            AssertObject(objects[0], id, DataObjectType.Transaction, "{'ghi':'jkl'}");
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
        public async Task DeleteObject_VersionConflict()
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

        [Fact]
        public async Task DeleteObject_RepeatableReadConflict()
        {
            long id = await this.storage.CreateObject(accounts[0], DataObjectType.Transaction, "{'abc':'def'}");

            using (DbTransaction transaction = this.storage.StartTransaction())
            {
                IReadOnlyList<DataObject> initialObject = await this.storage.GetObjects(accounts[0], new[] { id });

                await (await CreateStorageEngine()).UpdateObject(id, "{'ghi':'jkl'}", initialObject[0].Version);

                UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                    this.storage.DeleteObject(id, initialObject[0].Version));
            }

            IReadOnlyList<DataObject> objects = await this.storage.GetObjects(accounts[0], new[] { id });

            Assert.Equal(1, objects.Count);
            AssertObject(objects[0], id, DataObjectType.Transaction, "{'ghi':'jkl'}");
        }

        #endregion

        [Fact]
        public async Task StartTransaction_Atomicity()
        {
            long id1 = await this.storage.CreateObject(accounts[0], DataObjectType.Transaction, "{'abc':'def'}");
            IReadOnlyList<DataObject> initialObject = await this.storage.GetObjects(accounts[0], new[] { id1 });

            long id2;
            using (this.storage.StartTransaction())
            {
                id2 = await this.storage.CreateObject(accounts[0], DataObjectType.Transaction, "{'abc':'def'}");

                UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                    this.storage.UpdateObject(id1, "{'ghi':'jkl'}", versions[0]));
            }

            IReadOnlyList<DataObject> object1 = await this.storage.GetObjects(accounts[0], new[] { id1 });
            IReadOnlyList<DataObject> object2 = await this.storage.GetObjects(accounts[0], new[] { id2 });

            Assert.Equal(1, object1.Count);
            AssertObject(object1[0], id1, DataObjectType.Transaction, "{'abc':'def'}");
            Assert.Equal(initialObject[0].Version, object1[0].Version);
            Assert.Equal(0, object2.Count);
        }

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

        private static void AssertObject(DataObject dataObject, long id, DataObjectType type, string payload)
        {
            Assert.Equal(id, dataObject.Id);
            Assert.Equal(DataObjectType.Transaction, dataObject.Type);
            Assert.Equal(JObject.Parse(payload).ToString(Newtonsoft.Json.Formatting.None), JObject.Parse(dataObject.Payload).ToString(Newtonsoft.Json.Formatting.None));
        }

        #endregion
    }
}
