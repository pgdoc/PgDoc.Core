// Copyright 2016 Flavien Charlon
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Npgsql;
using Xunit;

namespace PgDoc.Tests
{
    public class SqlDocumentStoreTests : IDisposable
    {
        private const bool Update = false, Insert = true;
        private const bool ChangeBody = false, CheckVersion = true;

        private static readonly Guid[] _ids =
            Enumerable.Range(0, 32).Select(index => new Guid(Enumerable.Range(0, 16).Select(i => (byte)index).ToArray())).ToArray();

        private readonly SqlDocumentStore _store;

        public SqlDocumentStoreTests()
        {
            NpgsqlConnection connection = new NpgsqlConnection(ConfigurationManager.GetSetting("connection_string"));

            _store = new SqlDocumentStore(connection);
            _store.Initialize().Wait();

            NpgsqlCommand command = connection.CreateCommand();
            command.CommandText = @"TRUNCATE TABLE document;";
            command.ExecuteNonQuery();
        }

        #region Constructor

        [Fact]
        public void Constructor_Exception()
        {
            Assert.Throws<ArgumentNullException>(
                () => new SqlDocumentStore(null));
        }

        #endregion

        #region UpdateDocument

        [Fact]
        public async Task UpdateDocuments_Exception()
        {
            PostgresException exception = await Assert.ThrowsAsync<PostgresException>(
                () => UpdateDocument("{\"abc\":}", 0));

            Assert.Equal("22P02", exception.SqlState);
        }

        [Theory]
        [InlineData("{\"abc\":\"def\"}")]
        [InlineData(null)]
        public async Task UpdateDocuments_EmptyToValue(string to)
        {
            await UpdateDocument(to, 0);

            Document document = await _store.GetDocument(_ids[0]);

            AssertDocument(document, _ids[0], to, 1);
        }

        [Theory]
        [InlineData("{\"abc\":\"def\"}", "{\"ghi\":\"jkl\"}")]
        [InlineData(null, "{\"ghi\":\"jkl\"}")]
        [InlineData("{\"abc\":\"def\"}", null)]
        [InlineData(null, null)]
        public async Task UpdateDocuments_ValueToValue(string from, string to)
        {
            await UpdateDocument(from, 0);
            await UpdateDocument(to, 1);

            Document document = await _store.GetDocument(_ids[0]);

            AssertDocument(document, _ids[0], to, 2);
        }

        [Fact]
        public async Task UpdateDocuments_EmptyToCheck()
        {
            await CheckDocument(0);

            Document document = await _store.GetDocument(_ids[0]);

            AssertDocument(document, _ids[0], null, 0);
        }

        [Theory]
        [InlineData("{\"abc\":\"def\"}")]
        [InlineData(null)]
        public async Task UpdateDocuments_ValueToCheck(string from)
        {
            await UpdateDocument(from, 0);
            await CheckDocument(1);

            Document document = await _store.GetDocument(_ids[0]);

            AssertDocument(document, _ids[0], from, 1);
        }

        [Theory]
        [InlineData("{\"abc\":\"def\"}")]
        [InlineData(null)]
        public async Task UpdateDocuments_CheckToValue(string to)
        {
            await CheckDocument(0);
            await UpdateDocument(to, 0);

            Document document = await _store.GetDocument(_ids[0]);

            AssertDocument(document, _ids[0], to, 1);
        }

        [Fact]
        public async Task UpdateDocuments_CheckToCheck()
        {
            await CheckDocument(0);
            await CheckDocument(0);

            Document document = await _store.GetDocument(_ids[0]);

            AssertDocument(document, _ids[0], null, 0);
        }

        [Theory]
        [InlineData(CheckVersion)]
        [InlineData(ChangeBody)]
        public async Task UpdateDocuments_ConflictDocumentDoesNotExist(bool checkOnly)
        {
            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                checkOnly
                ? CheckDocument(10)
                : UpdateDocument("{\"abc\":\"def\"}", 10));

            Document document = await _store.GetDocument(_ids[0]);

            AssertDocument(document, _ids[0], null, 0);
            Assert.Equal(_ids[0], exception.Id);
            Assert.Equal(10, exception.Version);
        }

        [Theory]
        [InlineData(CheckVersion)]
        [InlineData(ChangeBody)]
        public async Task UpdateDocuments_ConflictWrongVersion(bool checkOnly)
        {
            await UpdateDocument("{\"abc\":\"def\"}", 0);

            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                checkOnly
                ? CheckDocument(10)
                : UpdateDocument("{\"ghi\":\"jkl\"}", 10));

            Document document = await _store.GetDocument(_ids[0]);

            AssertDocument(document, _ids[0], "{\"abc\":\"def\"}", 1);
            Assert.Equal(_ids[0], exception.Id);
            Assert.Equal(10, exception.Version);
        }

        [Theory]
        [InlineData(CheckVersion)]
        [InlineData(ChangeBody)]
        public async Task UpdateDocuments_ConflictDocumentAlreadyExists(bool checkOnly)
        {
            await UpdateDocument("{\"abc\":\"def\"}", 0);

            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                checkOnly
                ? CheckDocument(0)
                : UpdateDocument("{\"ghi\":\"jkl\"}", 0));

            Document document = await _store.GetDocument(_ids[0]);

            AssertDocument(document, _ids[0], "{\"abc\":\"def\"}", 1);
            Assert.Equal(_ids[0], exception.Id);
            Assert.Equal(0, exception.Version);
        }

        [Fact]
        public async Task UpdateDocuments_MultipleDocumentsSuccess()
        {
            await UpdateDocument(_ids[0], "{}", 0);
            await UpdateDocument(_ids[0], "{\"abc\":\"def\"}", 1);
            await UpdateDocument(_ids[1], "{\"ghi\":\"jkl\"}", 0);
            
            await _store.UpdateDocuments(
                new Document[]
                {
                    new Document(_ids[0], "{\"v\":\"1\"}", 2),
                    new Document(_ids[2], "{\"v\":\"2\"}", 0)
                },
                new Document[]
                {
                    new Document(_ids[1], "{\"v\":\"3\"}", 1),
                    new Document(_ids[3], "{\"v\":\"4\"}", 0)
                });

            Document document1 = await _store.GetDocument(_ids[0]);
            Document document2 = await _store.GetDocument(_ids[1]);
            Document document3 = await _store.GetDocument(_ids[2]);
            Document document4 = await _store.GetDocument(_ids[3]);

            AssertDocument(document1, _ids[0], "{\"v\":\"1\"}", 3);
            AssertDocument(document2, _ids[1], "{\"ghi\":\"jkl\"}", 1);
            AssertDocument(document3, _ids[2], "{\"v\":\"2\"}", 1);
            AssertDocument(document4, _ids[3], null, 0);
        }

        [Theory]
        [InlineData(CheckVersion)]
        [InlineData(ChangeBody)]
        public async Task UpdateDocuments_MultipleDocumentsConflict(bool checkOnly)
        {
            await UpdateDocument(_ids[0], "{\"abc\":\"def\"}", 0);

            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(async delegate ()
            {
                if (checkOnly)
                    await _store.UpdateDocuments(
                        new Document[] { new Document(_ids[0], "{\"ghi\":\"jkl\"}", 1) },
                        new Document[] { new Document(_ids[1], "{\"mno\":\"pqr\"}", 10) });
                else
                    await _store.UpdateDocuments(
                        new Document(_ids[0], "{\"ghi\":\"jkl\"}", 1),
                        new Document(_ids[1], "{\"mno\":\"pqr\"}", 10));
            });

            Document document1 = await _store.GetDocument(_ids[0]);
            Document document2 = await _store.GetDocument(_ids[1]);

            AssertDocument(document1, _ids[0], "{\"abc\":\"def\"}", 1);
            AssertDocument(document2, _ids[1], null, 0);
            Assert.Equal(_ids[1], exception.Id);
            Assert.Equal(10, exception.Version);
        }

        #endregion

        #region GetDocuments

        [Fact]
        public async Task GetDocuments_SingleDocument()
        {
            await UpdateDocument("{\"abc\":\"def\"}", 0);

            IReadOnlyList<Document> documents = await _store.GetDocuments(new[] { _ids[0] });

            Assert.Equal(1, documents.Count);
            AssertDocument(documents[0], _ids[0], "{\"abc\":\"def\"}", 1);
        }

        [Fact]
        public async Task GetDocuments_MultipleDocuments()
        {
            await UpdateDocument("{\"abc\":\"def\"}", 0);

            IReadOnlyList<Document> documents = await _store.GetDocuments(new[] { _ids[0], _ids[2], _ids[0], _ids[1] });

            Assert.Equal(4, documents.Count);
            AssertDocument(documents[0], _ids[0], "{\"abc\":\"def\"}", 1);
            AssertDocument(documents[1], _ids[2], null, 0);
            AssertDocument(documents[2], _ids[0], "{\"abc\":\"def\"}", 1);
            AssertDocument(documents[3], _ids[1], null, 0);
        }

        [Fact]
        public async Task GetDocuments_NoDocument()
        {
            IReadOnlyList<Document> documents = await _store.GetDocuments(new Guid[0]);

            Assert.Equal(0, documents.Count);
        }

        #endregion

        #region Concurrency

        [Fact]
        public async Task StartTransaction_Atomicity()
        {
            await UpdateDocument("{\"abc\":\"def\"}", 0);

            using (_store.StartTransaction(IsolationLevel.ReadCommitted))
            {
                await UpdateDocument(_ids[1], "{\"ghi\":\"jkl\"}", 0);

                UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                    UpdateDocument("{\"mno\":\"pqr\"}", 10));
            }

            Document document1 = await _store.GetDocument(_ids[0]);
            Document document2 = await _store.GetDocument(_ids[1]);

            AssertDocument(document1, _ids[0], "{\"abc\":\"def\"}", 1);
            AssertDocument(document2, _ids[1], null, 0);
        }

        [Theory]
        // Attempting to modify a document that has been modified outside of the transaction
        [InlineData(ChangeBody, Update)]
        [InlineData(ChangeBody, Insert)]
        // Attempting to read a document that has been modified outside of the transaction
        [InlineData(CheckVersion, Update)]
        [InlineData(CheckVersion, Insert)]
        public async Task UpdateDocuments_SerializationFailure(bool checkOnly, bool isInsert)
        {
            long initialVersion = isInsert ? 0 : await UpdateDocument("{\"abc\":\"def\"}", 0);
            long updatedVersion;
            long transactionVersion;
            UpdateConflictException exception;

            SqlDocumentStore connection1 = await CreateDocumentStore();
            SqlDocumentStore connection2 = await CreateDocumentStore();
            using (DbTransaction transaction = connection1.StartTransaction(IsolationLevel.RepeatableRead))
            {
                // Start transaction 1
                await connection1.GetDocument(_ids[0]);

                // Update the document with transaction 2
                updatedVersion = await UpdateDocument("{\"ghi\":\"jkl\"}", initialVersion, connection2);

                // Read the document with transaction 1, as if it was still unmodified
                transactionVersion = (await connection1.GetDocument(_ids[0])).Version;

                // Try to update or check the version of the document with transaction 1
                exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                    checkOnly
                    ? CheckDocument(transactionVersion, connection1)
                    : UpdateDocument("{\"mno\":\"pqr\"}", transactionVersion, connection1));
            }

            Document document = await _store.GetDocument(_ids[0]);

            Assert.Equal(initialVersion, transactionVersion);
            AssertDocument(document, _ids[0], "{\"ghi\":\"jkl\"}", updatedVersion);
            Assert.Equal(_ids[0], exception.Id);
            Assert.Equal(initialVersion, exception.Version);
        }

        [Fact]
        public async Task UpdateDocuments_DeadlockDetected()
        {
            await UpdateDocument("{\"abc\":\"def\"}", 0);
            Task update1;
            Task update2;

            SqlDocumentStore connection1 = await CreateDocumentStore();
            SqlDocumentStore connection2 = await CreateDocumentStore();
            using (DbTransaction transaction1 = connection1.StartTransaction(IsolationLevel.ReadCommitted))
            using (DbTransaction transaction2 = connection2.StartTransaction(IsolationLevel.ReadCommitted))
            {
                // Lock the document with both transactions
                await CheckDocument(1, connection1);
                await CheckDocument(1, connection2);

                // Try to update the document with both transactions
                update1 = UpdateDocument("{\"ghi\":\"jkl\"}", 1, connection1);
                update2 = UpdateDocument("{\"mno\":\"pqr\"}", 1, connection2);

                // One transaction succeeds and the other is terminated
                await Task.WhenAny(update1);
                await Task.WhenAny(update2);

                transaction1.Commit();
                transaction2.Commit();
            }

            Document document = await _store.GetDocument(_ids[0]);

            UpdateConflictException exception;
            if (update1.Status == TaskStatus.Faulted)
            {
                // Transaction 2 succeeded
                AssertDocument(document, _ids[0], "{\"mno\":\"pqr\"}", 2);
                exception = update1.Exception.InnerException as UpdateConflictException;
            }
            else
            {
                // Transaction 1 succeeded
                AssertDocument(document, _ids[0], "{\"ghi\":\"jkl\"}", 2);
                exception = update2.Exception.InnerException as UpdateConflictException;
            }

            Assert.NotNull(exception);
            Assert.Equal(_ids[0], exception.Id);
            Assert.Equal(1, exception.Version);
        }

        [Theory]
        // Write operation waiting for write lock
        [InlineData(false, ChangeBody, Update)]
        [InlineData(false, ChangeBody, Insert)]
        // Read operation waiting for write lock
        [InlineData(false, CheckVersion, Update)]
        [InlineData(false, CheckVersion, Insert)]
        // Write operation waiting for read lock
        [InlineData(true, ChangeBody, Update)]
        [InlineData(true, ChangeBody, Insert)]
        public async Task UpdateDocuments_WaitForLock(bool isReadLock, bool checkOnly, bool isInsert)
        {
            long initialVersion = isInsert ? 0 : await UpdateDocument("{\"abc\":\"def\"}", 0);
            long updatedVersion;
            PostgresException exception;

            SqlDocumentStore connection1 = await CreateDocumentStore(shortTimeout: true);
            SqlDocumentStore connection2 = await CreateDocumentStore(shortTimeout: true);
            using (DbTransaction transaction = connection1.StartTransaction(IsolationLevel.ReadCommitted))
            {
                // Lock the document with transaction 1
                updatedVersion =
                    isReadLock
                    ? await CheckDocument(initialVersion, connection1)
                    : await UpdateDocument("{\"ghi\":\"jkl\"}", initialVersion, connection1);

                // Try to update or check the version of the document with transaction 2
                exception = await Assert.ThrowsAsync<PostgresException>(() =>
                    checkOnly
                    ? CheckDocument(initialVersion, connection2)
                    : UpdateDocument("{\"mno\":\"pqr\"}", initialVersion, connection2));

                transaction.Commit();
            }

            Document document = await _store.GetDocument(_ids[0]);

            Assert.Equal("57014", exception.SqlState);

            if (isReadLock)
                AssertDocument(document, _ids[0], isInsert ? null : "{\"abc\":\"def\"}", initialVersion);
            else
                AssertDocument(document, _ids[0], "{\"ghi\":\"jkl\"}", updatedVersion);
        }

        [Fact]
        public async Task UpdateDocuments_ConcurrentReadLock()
        {
            await UpdateDocument("{\"abc\":\"def\"}", 0);

            SqlDocumentStore connection1 = await CreateDocumentStore();
            SqlDocumentStore connection2 = await CreateDocumentStore();
            using (DbTransaction transaction = connection1.StartTransaction(IsolationLevel.ReadCommitted))
            {
                // Lock the document for read with transaction 1
                await CheckDocument(1, connection1);

                // Check the version of the document with transaction 2
                await CheckDocument(1, connection2);

                transaction.Commit();
            }

            Document document = await _store.GetDocument(_ids[0]);

            AssertDocument(document, _ids[0], "{\"abc\":\"def\"}", 1);
        }

        #endregion

        public void Dispose()
        {
            _store.Dispose();
        }

        #region Helper Methods

        private static async Task<SqlDocumentStore> CreateDocumentStore(bool shortTimeout = false)
        {
            NpgsqlConnection connection = new NpgsqlConnection(ConfigurationManager.GetSetting("connection_string"));

            SqlDocumentStore engine = new SqlDocumentStore(connection);
            await engine.Initialize();

            if (shortTimeout)
            {
                NpgsqlCommand command = connection.CreateCommand();
                command.CommandText = @"SET statement_timeout TO 500;";
                await command.ExecuteNonQueryAsync();
            }

            return engine;
        }

        private async Task<long> UpdateDocument(Guid id, string body, long version)
        {
            await _store.UpdateDocuments(new Document(id, body, version));

            return version + 1;
        }

        private async Task<long> UpdateDocument(string body, long version, SqlDocumentStore store = null)
        {
            if (store == null)
                store = _store;

            await store.UpdateDocuments(new Document(_ids[0], body, version));

            return version + 1;
        }

        private async Task<long> CheckDocument(long version, SqlDocumentStore store = null)
        {
            if (store == null)
                store = _store;

            await store.UpdateDocuments(new Document[0], new[] { new Document(_ids[0], "{\"ignored\":\"ignored\"}", version) });

            return version + 1;
        }

        private static void AssertDocument(Document document, Guid id, string body, long version)
        {
            Assert.Equal(id, document.Id);

            if (body == null)
            {
                Assert.Null(document.Body);
            }
            else
            {
                Assert.NotNull(document.Body);
                Assert.Equal(JObject.Parse(body).ToString(Newtonsoft.Json.Formatting.None), JObject.Parse(document.Body).ToString(Newtonsoft.Json.Formatting.None));
            }

            Assert.Equal(version, document.Version);
        }

        #endregion
    }
}
