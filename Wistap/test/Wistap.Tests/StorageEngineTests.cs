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
        private const bool Update = false, Insert = true;
        private const bool ChangeValue = false, CheckVersion = true;

        private static readonly ByteString wrongVersion = new ByteString(Enumerable.Range(0, 32).Select(i => (byte)255));
        private static readonly DocumentId[] ids =
            Enumerable.Range(0, 32).Select(index => new DocumentId(new Guid(Enumerable.Range(0, 16).Select(i => (byte)index).ToArray()))).ToArray();

        private readonly StorageEngine storage;

        public StorageEngineTests()
        {
            NpgsqlConnection connection = new NpgsqlConnection("Server=127.0.0.1;Port=5432;Database=wistap;User Id=postgres;Password=admin;CommandTimeout=1");
            this.storage = new StorageEngine(connection);
            this.storage.Initialize().Wait();

            NpgsqlCommand command = connection.CreateCommand();
            command.CommandText = @"TRUNCATE TABLE wistap.document;";
            command.ExecuteNonQuery();
        }

        #region UpdateDocument

        [Theory]
        [InlineData("{'abc':'def'}")]
        [InlineData(null)]
        public async Task UpdateDocuments_EmptyToValue(string to)
        {
            ByteString version = await UpdateDocument(to, ByteString.Empty);

            Document dataObject = await this.storage.GetDocument(ids[0]);

            AssertDocument(dataObject, ids[0], to, version);
            Assert.Equal(8, version.Value.Count);
        }

        [Theory]
        [InlineData("{'abc':'def'}", "{'ghi':'jkl'}")]
        [InlineData(null, "{'ghi':'jkl'}")]
        [InlineData("{'abc':'def'}", null)]
        [InlineData(null, null)]
        public async Task UpdateDocuments_ValueToValue(string from, string to)
        {
            ByteString version1 = await UpdateDocument(from, ByteString.Empty);
            ByteString version2 = await UpdateDocument(to, version1);

            Document dataObject = await this.storage.GetDocument(ids[0]);

            AssertDocument(dataObject, ids[0], to, version2);
            Assert.Equal(8, version2.Value.Count);
            Assert.NotEqual(version1, version2);
        }

        [Fact]
        public async Task UpdateDocuments_EmptyToCheck()
        {
            ByteString version = await CheckDocument(ByteString.Empty);

            Document dataObject = await this.storage.GetDocument(ids[0]);

            AssertDocument(dataObject, ids[0], null, ByteString.Empty);
            Assert.Equal(8, version.Value.Count);
        }

        [Theory]
        [InlineData("{'abc':'def'}")]
        [InlineData(null)]
        public async Task UpdateDocuments_ValueToCheck(string from)
        {
            ByteString version1 = await UpdateDocument(from, ByteString.Empty);
            ByteString version2 = await CheckDocument(version1);

            Document dataObject = await this.storage.GetDocument(ids[0]);

            AssertDocument(dataObject, ids[0], from, version1);
            Assert.Equal(8, version2.Value.Count);
            Assert.NotEqual(version1, version2);
        }

        [Theory]
        [InlineData("{'abc':'def'}")]
        [InlineData(null)]
        public async Task UpdateDocuments_CheckToValue(string to)
        {
            await CheckDocument(ByteString.Empty);
            ByteString version = await UpdateDocument(to, ByteString.Empty);

            Document dataObject = await this.storage.GetDocument(ids[0]);

            AssertDocument(dataObject, ids[0], to, version);
            Assert.Equal(8, version.Value.Count);
        }

        [Fact]
        public async Task UpdateDocuments_CheckToCheck()
        {
            await CheckDocument(ByteString.Empty);
            await CheckDocument(ByteString.Empty);

            Document dataObject = await this.storage.GetDocument(ids[0]);

            AssertDocument(dataObject, ids[0], null, ByteString.Empty);
        }

        [Theory]
        [InlineData(CheckVersion)]
        [InlineData(ChangeValue)]
        public async Task UpdateDocuments_ConflictDocumentDoesNotExist(bool checkOnly)
        {
            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                checkOnly
                ? CheckDocument(wrongVersion)
                : UpdateDocument("{'abc':'def'}", wrongVersion));

            Document dataObject = await this.storage.GetDocument(ids[0]);

            AssertDocument(dataObject, ids[0], null, ByteString.Empty);
            Assert.Equal(ids[0], exception.Id);
            Assert.Equal(wrongVersion, exception.Version);
        }

        [Theory]
        [InlineData(CheckVersion)]
        [InlineData(ChangeValue)]
        public async Task UpdateDocuments_ConflictWrongVersion(bool checkOnly)
        {
            ByteString version1 = await UpdateDocument("{'abc':'def'}", ByteString.Empty);

            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                checkOnly
                ? CheckDocument(wrongVersion)
                : UpdateDocument("{'ghi':'jkl'}", wrongVersion));

            Document dataObject = await this.storage.GetDocument(ids[0]);

            AssertDocument(dataObject, ids[0], "{'abc':'def'}", version1);
            Assert.Equal(ids[0], exception.Id);
            Assert.Equal(wrongVersion, exception.Version);
        }

        [Theory]
        [InlineData(CheckVersion)]
        [InlineData(ChangeValue)]
        public async Task UpdateDocuments_ConflictDocumentAlreadyExists(bool checkOnly)
        {
            ByteString version1 = await UpdateDocument("{'abc':'def'}", ByteString.Empty);

            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                checkOnly
                ? CheckDocument(ByteString.Empty)
                : UpdateDocument("{'ghi':'jkl'}", ByteString.Empty));

            Document dataObject = await this.storage.GetDocument(ids[0]);

            AssertDocument(dataObject, ids[0], "{'abc':'def'}", version1);
            Assert.Equal(ids[0], exception.Id);
            Assert.Equal(ByteString.Empty, exception.Version);
        }

        [Fact]
        public async Task UpdateDocuments_MultipleDocumentsSuccess()
        {
            ByteString version1 = await this.storage.UpdateDocument(ids[0], "{'abc':'def'}", ByteString.Empty);
            ByteString version2 = await this.storage.UpdateDocument(ids[1], "{'ghi':'jkl'}", ByteString.Empty);

            ByteString version3 = await this.storage.UpdateDocuments(
                new Document[]
                {
                    new Document(ids[0], "{'v':'1'}", version1),
                    new Document(ids[2], "{'v':'2'}", ByteString.Empty)
                },
                new Document[]
                {
                    new Document(ids[1], "{'v':'3'}", version2),
                    new Document(ids[3], "{'v':'4'}", ByteString.Empty)
                });

            Document object1 = await this.storage.GetDocument(ids[0]);
            Document object2 = await this.storage.GetDocument(ids[1]);
            Document object3 = await this.storage.GetDocument(ids[2]);
            Document object4 = await this.storage.GetDocument(ids[3]);

            AssertDocument(object1, ids[0], "{'v':'1'}", version3);
            AssertDocument(object2, ids[1], "{'ghi':'jkl'}", version2);
            AssertDocument(object3, ids[2], "{'v':'2'}", version3);
            AssertDocument(object4, ids[3], null, ByteString.Empty);
        }

        [Theory]
        [InlineData(CheckVersion)]
        [InlineData(ChangeValue)]
        public async Task UpdateDocuments_MultipleDocumentsConflict(bool checkOnly)
        {
            ByteString version1 = await this.storage.UpdateDocument(ids[0], "{'abc':'def'}", ByteString.Empty);

            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(async delegate ()
            {
                if (checkOnly)
                    await this.storage.UpdateDocuments(
                        new Document[] { new Document(ids[0], "{'ghi':'jkl'}", version1) },
                        new Document[] { new Document(ids[1], "{'mno':'pqr'}", wrongVersion) });
                else
                    await this.storage.UpdateDocuments(
                        new Document(ids[0], "{'ghi':'jkl'}", version1),
                        new Document(ids[1], "{'mno':'pqr'}", wrongVersion));
            });

            Document object1 = await this.storage.GetDocument(ids[0]);
            Document object2 = await this.storage.GetDocument(ids[1]);

            AssertDocument(object1, ids[0], "{'abc':'def'}", version1);
            AssertDocument(object2, ids[1], null, ByteString.Empty);
            Assert.Equal(ids[1], exception.Id);
            Assert.Equal(wrongVersion, exception.Version);
        }

        #endregion

        #region GetDocuments

        [Fact]
        public async Task GetDocuments_SingleDocument()
        {
            ByteString version1 = await UpdateDocument("{'abc':'def'}", ByteString.Empty);

            IReadOnlyList<Document> objects = await this.storage.GetDocuments(new[] { ids[0] });

            Assert.Equal(1, objects.Count);
            AssertDocument(objects.First(dataObject => dataObject.Id.Equals(ids[0])), ids[0], "{'abc':'def'}", version1);
        }

        [Fact]
        public async Task GetDocuments_MultipleDocuments()
        {
            ByteString version1 = await UpdateDocument("{'abc':'def'}", ByteString.Empty);

            IReadOnlyList<Document> objects = await this.storage.GetDocuments(new[] { ids[0], ids[1] });

            Assert.Equal(2, objects.Count);
            AssertDocument(objects.First(dataObject => dataObject.Id.Equals(ids[0])), ids[0], "{'abc':'def'}", version1);
            AssertDocument(objects.First(dataObject => dataObject.Id.Equals(ids[1])), ids[1], null, ByteString.Empty);
        }

        [Fact]
        public async Task GetDocuments_NoDocument()
        {
            IReadOnlyList<Document> objects = await this.storage.GetDocuments(new DocumentId[0]);

            Assert.Equal(0, objects.Count);
        }

        #endregion

        #region Concurrency

        [Fact]
        public async Task StartTransaction_Atomicity()
        {
            ByteString version1 = await UpdateDocument("{'abc':'def'}", ByteString.Empty);

            using (this.storage.StartTransaction())
            {
                await this.storage.UpdateDocument(ids[1], "{'ghi':'jkl'}", ByteString.Empty);

                UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                    UpdateDocument("{'mno':'pqr'}", wrongVersion));
            }

            Document object1 = await this.storage.GetDocument(ids[0]);
            Document object2 = await this.storage.GetDocument(ids[1]);

            AssertDocument(object1, ids[0], "{'abc':'def'}", version1);
            AssertDocument(object2, ids[1], null, ByteString.Empty);
        }

        [Theory]
        // Attempting to modify an object that has been modified outside of the transaction
        [InlineData(ChangeValue, Update)]
        [InlineData(ChangeValue, Insert)]
        // Attempting to read an object that has been modified outside of the transaction
        [InlineData(CheckVersion, Update)]
        [InlineData(CheckVersion, Insert)]
        public async Task UpdateDocuments_SerializationError(bool checkOnly, bool isInsert)
        {
            ByteString initialVersion = isInsert ? ByteString.Empty : await UpdateDocument("{'abc':'def'}", ByteString.Empty);
            ByteString updatedVersion;
            UpdateConflictException exception;

            using (DbTransaction transaction = this.storage.StartTransaction())
            {
                // Start transaction 1
                await this.storage.GetDocument(ids[1]);

                // Update the object with transaction 2
                updatedVersion = await (await CreateStorageEngine()).UpdateDocument(ids[0], "{'ghi':'jkl'}", initialVersion);

                // Try to update or check the version of the object with transaction 1
                exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                    checkOnly
                    ? CheckDocument(initialVersion)
                    : UpdateDocument("{'mno':'pqr'}", initialVersion));
            }

            Document dataObject = await this.storage.GetDocument(ids[0]);

            AssertDocument(dataObject, ids[0], "{'ghi':'jkl'}", updatedVersion);
            Assert.Equal(ids[0], exception.Id);
            Assert.Equal(initialVersion, exception.Version);
        }

        [Theory]
        // Write operation waiting for write lock
        [InlineData(false, ChangeValue, Update)]
        [InlineData(false, ChangeValue, Insert)]
        // Read operation waiting for write lock
        [InlineData(false, CheckVersion, Update)]
        [InlineData(false, CheckVersion, Insert)]
        // Write operation waiting for read lock
        [InlineData(true, ChangeValue, Update)]
        [InlineData(true, ChangeValue, Insert)]
        public async Task UpdateDocuments_WaitForLock(bool isReadLock, bool checkOnly, bool isInsert)
        {
            ByteString initialVersion = isInsert ? ByteString.Empty : await UpdateDocument("{'abc':'def'}", ByteString.Empty);
            ByteString updatedVersion;

            StorageEngine connection2 = await CreateStorageEngine();
            using (DbTransaction transaction = connection2.StartTransaction())
            {
                // Lock the object with transaction 2
                updatedVersion =
                    isReadLock
                    ? await connection2.UpdateDocuments(new Document[0], new[] { new Document(ids[0], "{'ignored':'ignored'}", initialVersion) })
                    : await connection2.UpdateDocument(ids[0], "{'ghi':'jkl'}", initialVersion);

                // Try to update or check the version of the object with transaction 1
                await Assert.ThrowsAsync<TaskCanceledException>(() =>
                    checkOnly
                    ? CheckDocument(initialVersion)
                    : UpdateDocument("{'mno':'pqr'}", initialVersion));

                transaction.Commit();
            }

            Document dataObject = await this.storage.GetDocument(ids[0]);

            if (isReadLock)
                AssertDocument(dataObject, ids[0], isInsert ? null : "{'abc':'def'}", initialVersion);
            else
                AssertDocument(dataObject, ids[0], "{'ghi':'jkl'}", updatedVersion);
        }

        [Fact]
        public async Task UpdateDocuments_ConcurrentReadLock()
        {
            ByteString initialVersion = await UpdateDocument("{'abc':'def'}", ByteString.Empty);

            StorageEngine connection2 = await CreateStorageEngine();
            using (DbTransaction transaction = connection2.StartTransaction())
            {
                // Lock the object for read with transaction 2
                await connection2.UpdateDocuments(new Document[0], new[] { new Document(ids[0], "{'ignored':'ignored'}", initialVersion) });

                // Check the version of the object with transaction 1
                await CheckDocument(initialVersion);

                transaction.Commit();
            }

            Document dataObject = await this.storage.GetDocument(ids[0]);

            AssertDocument(dataObject, ids[0], "{'abc':'def'}", initialVersion);
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

        private async Task<ByteString> UpdateDocument(string value, ByteString version)
        {
            return await this.storage.UpdateDocument(ids[0], value, version);
        }

        private async Task<ByteString> CheckDocument(ByteString version)
        {
            return await this.storage.UpdateDocuments(new Document[0], new[] { new Document(ids[0], "{'ignored':'ignored'}", version) });
        }

        private static void AssertDocument(Document document, DocumentId id, string value, ByteString version)
        {
            Assert.Equal(id.Value, document.Id.Value);

            if (value == null)
            {
                Assert.Null(document.Content);
            }
            else
            {
                Assert.NotNull(document.Content);
                Assert.Equal(JObject.Parse(value).ToString(Newtonsoft.Json.Formatting.None), JObject.Parse(document.Content).ToString(Newtonsoft.Json.Formatting.None));
            }

            Assert.Equal(version, document.Version);
        }

        #endregion
    }
}
