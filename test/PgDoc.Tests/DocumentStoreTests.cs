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
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Npgsql;
using Xunit;

namespace PgDoc.Tests
{
    public class DocumentStoreTests : IDisposable
    {
        private const bool Update = false, Insert = true;
        private const bool ChangeBody = false, CheckVersion = true;

        private static readonly ByteString _wrongVersion = new ByteString(Enumerable.Range(0, 32).Select(i => (byte)255).ToArray());
        private static readonly Guid[] _ids =
            Enumerable.Range(0, 32).Select(index => new Guid(Enumerable.Range(0, 16).Select(i => (byte)index).ToArray())).ToArray();

        private readonly DocumentStore _store;

        public DocumentStoreTests()
        {
            NpgsqlConnection connection = new NpgsqlConnection(ConfigurationManager.GetSetting("connection_string"));

            _store = new DocumentStore(connection);
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
                () => new DocumentStore(null));
        }

        #endregion

        #region UpdateDocument

        [Fact]
        public async Task UpdateDocuments_Exception()
        {
            PostgresException exception = await Assert.ThrowsAsync<PostgresException>(
                () => UpdateDocument("{\"abc\":}", ByteString.Empty));

            Assert.Equal("22P02", exception.SqlState);
        }

        [Theory]
        [InlineData("{\"abc\":\"def\"}")]
        [InlineData(null)]
        public async Task UpdateDocuments_EmptyToValue(string to)
        {
            ByteString version = await UpdateDocument(to, ByteString.Empty);

            Document document = await _store.GetDocument(_ids[0]);

            AssertDocument(document, _ids[0], to, version);
            Assert.Equal(16, version.Value.Length);
        }

        [Theory]
        [InlineData("{\"abc\":\"def\"}", "{\"ghi\":\"jkl\"}")]
        [InlineData(null, "{\"ghi\":\"jkl\"}")]
        [InlineData("{\"abc\":\"def\"}", null)]
        [InlineData(null, null)]
        public async Task UpdateDocuments_ValueToValue(string from, string to)
        {
            ByteString version1 = await UpdateDocument(from, ByteString.Empty);
            ByteString version2 = await UpdateDocument(to, version1);

            Document document = await _store.GetDocument(_ids[0]);

            AssertDocument(document, _ids[0], to, version2);
            Assert.Equal(16, version2.Value.Length);
            Assert.NotEqual(version1, version2);
        }

        [Fact]
        public async Task UpdateDocuments_EmptyToCheck()
        {
            ByteString version = await CheckDocument(ByteString.Empty);

            Document document = await _store.GetDocument(_ids[0]);

            AssertDocument(document, _ids[0], null, ByteString.Empty);
            Assert.Equal(16, version.Value.Length);
        }

        [Theory]
        [InlineData("{\"abc\":\"def\"}")]
        [InlineData(null)]
        public async Task UpdateDocuments_ValueToCheck(string from)
        {
            ByteString version1 = await UpdateDocument(from, ByteString.Empty);
            ByteString version2 = await CheckDocument(version1);

            Document document = await _store.GetDocument(_ids[0]);

            AssertDocument(document, _ids[0], from, version1);
            Assert.Equal(16, version2.Value.Length);
            Assert.NotEqual(version1, version2);
        }

        [Theory]
        [InlineData("{\"abc\":\"def\"}")]
        [InlineData(null)]
        public async Task UpdateDocuments_CheckToValue(string to)
        {
            await CheckDocument(ByteString.Empty);
            ByteString version = await UpdateDocument(to, ByteString.Empty);

            Document document = await _store.GetDocument(_ids[0]);

            AssertDocument(document, _ids[0], to, version);
            Assert.Equal(16, version.Value.Length);
        }

        [Fact]
        public async Task UpdateDocuments_CheckToCheck()
        {
            await CheckDocument(ByteString.Empty);
            await CheckDocument(ByteString.Empty);

            Document document = await _store.GetDocument(_ids[0]);

            AssertDocument(document, _ids[0], null, ByteString.Empty);
        }

        [Theory]
        [InlineData(CheckVersion)]
        [InlineData(ChangeBody)]
        public async Task UpdateDocuments_ConflictDocumentDoesNotExist(bool checkOnly)
        {
            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                checkOnly
                ? CheckDocument(_wrongVersion)
                : UpdateDocument("{\"abc\":\"def\"}", _wrongVersion));

            Document document = await _store.GetDocument(_ids[0]);

            AssertDocument(document, _ids[0], null, ByteString.Empty);
            Assert.Equal(_ids[0], exception.Id);
            Assert.Equal(_wrongVersion, exception.Version);
        }

        [Theory]
        [InlineData(CheckVersion)]
        [InlineData(ChangeBody)]
        public async Task UpdateDocuments_ConflictWrongVersion(bool checkOnly)
        {
            ByteString version1 = await UpdateDocument("{\"abc\":\"def\"}", ByteString.Empty);

            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                checkOnly
                ? CheckDocument(_wrongVersion)
                : UpdateDocument("{\"ghi\":\"jkl\"}", _wrongVersion));

            Document document = await _store.GetDocument(_ids[0]);

            AssertDocument(document, _ids[0], "{\"abc\":\"def\"}", version1);
            Assert.Equal(_ids[0], exception.Id);
            Assert.Equal(_wrongVersion, exception.Version);
        }

        [Theory]
        [InlineData(CheckVersion)]
        [InlineData(ChangeBody)]
        public async Task UpdateDocuments_ConflictDocumentAlreadyExists(bool checkOnly)
        {
            ByteString version1 = await UpdateDocument("{\"abc\":\"def\"}", ByteString.Empty);

            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                checkOnly
                ? CheckDocument(ByteString.Empty)
                : UpdateDocument("{\"ghi\":\"jkl\"}", ByteString.Empty));

            Document document = await _store.GetDocument(_ids[0]);

            AssertDocument(document, _ids[0], "{\"abc\":\"def\"}", version1);
            Assert.Equal(_ids[0], exception.Id);
            Assert.Equal(ByteString.Empty, exception.Version);
        }

        [Fact]
        public async Task UpdateDocuments_MultipleDocumentsSuccess()
        {
            ByteString version1 = await _store.UpdateDocument(_ids[0], "{\"abc\":\"def\"}", ByteString.Empty);
            ByteString version2 = await _store.UpdateDocument(_ids[1], "{\"ghi\":\"jkl\"}", ByteString.Empty);

            ByteString version3 = await _store.UpdateDocuments(
                new Document[]
                {
                    new Document(_ids[0], "{\"v\":\"1\"}", version1),
                    new Document(_ids[2], "{\"v\":\"2\"}", ByteString.Empty)
                },
                new Document[]
                {
                    new Document(_ids[1], "{\"v\":\"3\"}", version2),
                    new Document(_ids[3], "{\"v\":\"4\"}", ByteString.Empty)
                });

            Document document1 = await _store.GetDocument(_ids[0]);
            Document document2 = await _store.GetDocument(_ids[1]);
            Document document3 = await _store.GetDocument(_ids[2]);
            Document document4 = await _store.GetDocument(_ids[3]);

            AssertDocument(document1, _ids[0], "{\"v\":\"1\"}", version3);
            AssertDocument(document2, _ids[1], "{\"ghi\":\"jkl\"}", version2);
            AssertDocument(document3, _ids[2], "{\"v\":\"2\"}", version3);
            AssertDocument(document4, _ids[3], null, ByteString.Empty);
        }

        [Theory]
        [InlineData(CheckVersion)]
        [InlineData(ChangeBody)]
        public async Task UpdateDocuments_MultipleDocumentsConflict(bool checkOnly)
        {
            ByteString version1 = await _store.UpdateDocument(_ids[0], "{\"abc\":\"def\"}", ByteString.Empty);

            UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(async delegate ()
            {
                if (checkOnly)
                    await _store.UpdateDocuments(
                        new Document[] { new Document(_ids[0], "{\"ghi\":\"jkl\"}", version1) },
                        new Document[] { new Document(_ids[1], "{\"mno\":\"pqr\"}", _wrongVersion) });
                else
                    await _store.UpdateDocuments(
                        new Document(_ids[0], "{\"ghi\":\"jkl\"}", version1),
                        new Document(_ids[1], "{\"mno\":\"pqr\"}", _wrongVersion));
            });

            Document document1 = await _store.GetDocument(_ids[0]);
            Document document2 = await _store.GetDocument(_ids[1]);

            AssertDocument(document1, _ids[0], "{\"abc\":\"def\"}", version1);
            AssertDocument(document2, _ids[1], null, ByteString.Empty);
            Assert.Equal(_ids[1], exception.Id);
            Assert.Equal(_wrongVersion, exception.Version);
        }

        #endregion

        #region GetDocuments

        [Fact]
        public async Task GetDocuments_SingleDocument()
        {
            ByteString version1 = await UpdateDocument("{\"abc\":\"def\"}", ByteString.Empty);

            IReadOnlyList<Document> documents = await _store.GetDocuments(new[] { _ids[0] });

            Assert.Equal(1, documents.Count);
            AssertDocument(documents[0], _ids[0], "{\"abc\":\"def\"}", version1);
        }

        [Fact]
        public async Task GetDocuments_MultipleDocuments()
        {
            ByteString version1 = await UpdateDocument("{\"abc\":\"def\"}", ByteString.Empty);

            IReadOnlyList<Document> documents = await _store.GetDocuments(new[] { _ids[0], _ids[2], _ids[0], _ids[1] });

            Assert.Equal(4, documents.Count);
            AssertDocument(documents[0], _ids[0], "{\"abc\":\"def\"}", version1);
            AssertDocument(documents[1], _ids[2], null, ByteString.Empty);
            AssertDocument(documents[2], _ids[0], "{\"abc\":\"def\"}", version1);
            AssertDocument(documents[3], _ids[1], null, ByteString.Empty);
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
            ByteString version1 = await UpdateDocument("{\"abc\":\"def\"}", ByteString.Empty);

            using (_store.StartTransaction(IsolationLevel.ReadCommitted))
            {
                await _store.UpdateDocument(_ids[1], "{\"ghi\":\"jkl\"}", ByteString.Empty);

                UpdateConflictException exception = await Assert.ThrowsAsync<UpdateConflictException>(() =>
                    UpdateDocument("{\"mno\":\"pqr\"}", _wrongVersion));
            }

            Document document1 = await _store.GetDocument(_ids[0]);
            Document document2 = await _store.GetDocument(_ids[1]);

            AssertDocument(document1, _ids[0], "{\"abc\":\"def\"}", version1);
            AssertDocument(document2, _ids[1], null, ByteString.Empty);
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
            ByteString initialVersion = isInsert ? ByteString.Empty : await UpdateDocument("{\"abc\":\"def\"}", ByteString.Empty);
            ByteString updatedVersion;
            ByteString transactionVersion;
            UpdateConflictException exception;

            DocumentStore connection1 = await CreateDocumentStore();
            DocumentStore connection2 = await CreateDocumentStore();
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
            ByteString initialVersion = await UpdateDocument("{\"abc\":\"def\"}", ByteString.Empty);
            Task<ByteString> update1;
            Task<ByteString> update2;

            DocumentStore connection1 = await CreateDocumentStore();
            DocumentStore connection2 = await CreateDocumentStore();
            using (DbTransaction transaction1 = connection1.StartTransaction(IsolationLevel.ReadCommitted))
            using (DbTransaction transaction2 = connection2.StartTransaction(IsolationLevel.ReadCommitted))
            {
                // Lock the document with both transactions
                await CheckDocument(initialVersion, connection1);
                await CheckDocument(initialVersion, connection2);

                // Try to update the document with both transactions
                update1 = UpdateDocument("{\"ghi\":\"jkl\"}", initialVersion, connection1);
                update2 = UpdateDocument("{\"mno\":\"pqr\"}", initialVersion, connection2);

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
                AssertDocument(document, _ids[0], "{\"mno\":\"pqr\"}", update2.Result);
                exception = update1.Exception.InnerException as UpdateConflictException;
            }
            else
            {
                // Transaction 1 succeeded
                AssertDocument(document, _ids[0], "{\"ghi\":\"jkl\"}", update1.Result);
                exception = update2.Exception.InnerException as UpdateConflictException;
            }

            Assert.NotNull(exception);
            Assert.Equal(_ids[0], exception.Id);
            Assert.Equal(initialVersion, exception.Version);
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
            ByteString initialVersion = isInsert ? ByteString.Empty : await UpdateDocument("{\"abc\":\"def\"}", ByteString.Empty);
            ByteString updatedVersion;
            PostgresException exception;

            DocumentStore connection1 = await CreateDocumentStore(shortTimeout: true);
            DocumentStore connection2 = await CreateDocumentStore(shortTimeout: true);
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
            ByteString initialVersion = await UpdateDocument("{\"abc\":\"def\"}", ByteString.Empty);

            DocumentStore connection1 = await CreateDocumentStore();
            DocumentStore connection2 = await CreateDocumentStore();
            using (DbTransaction transaction = connection1.StartTransaction(IsolationLevel.ReadCommitted))
            {
                // Lock the document for read with transaction 1
                await CheckDocument(initialVersion, connection1);

                // Check the version of the document with transaction 2
                await CheckDocument(initialVersion, connection2);

                transaction.Commit();
            }

            Document document = await _store.GetDocument(_ids[0]);

            AssertDocument(document, _ids[0], "{\"abc\":\"def\"}", initialVersion);
        }

        #endregion

        public void Dispose()
        {
            _store.Dispose();
        }

        #region Helper Methods

        private static async Task<DocumentStore> CreateDocumentStore(bool shortTimeout = false)
        {
            NpgsqlConnection connection = new NpgsqlConnection(ConfigurationManager.GetSetting("connection_string"));

            DocumentStore engine = new DocumentStore(connection);
            await engine.Initialize();

            if (shortTimeout)
            {
                NpgsqlCommand command = connection.CreateCommand();
                command.CommandText = @"SET statement_timeout TO 500;";
                await command.ExecuteNonQueryAsync();
            }

            return engine;
        }

        private async Task<ByteString> UpdateDocument(string body, ByteString version, DocumentStore store = null)
        {
            if (store == null)
                store = _store;

            return await store.UpdateDocument(_ids[0], body, version);
        }

        private async Task<ByteString> CheckDocument(ByteString version, DocumentStore store = null)
        {
            if (store == null)
                store = _store;

            return await store.UpdateDocuments(new Document[0], new[] { new Document(_ids[0], "{\"ignored\":\"ignored\"}", version) });
        }

        private static void AssertDocument(Document document, Guid id, string body, ByteString version)
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
