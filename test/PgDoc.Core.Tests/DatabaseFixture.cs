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

namespace PgDoc.Core.Tests;

using System.IO;
using System.Threading.Tasks;
using DotNet.Testcontainers.Containers.Builders;
using DotNet.Testcontainers.Containers.Configurations.Abstractions;
using DotNet.Testcontainers.Containers.Configurations.Databases;
using DotNet.Testcontainers.Containers.Modules.Databases;
using Npgsql;
using Xunit;

public class DatabaseFixture : IAsyncLifetime
{
    private PostgreSqlTestcontainer _container;

    public string ConnectionString { get => _container.ConnectionString; }

    public async Task InitializeAsync()
    {
        const string database = "db";
        const string username = "postgres";

        TestcontainerDatabaseConfiguration configuration = new PostgreSqlTestcontainerConfiguration("postgres:12")
        {
            Database = database,
            Username = username,
            Password = username,
        };

        _container = new TestcontainersBuilder<PostgreSqlTestcontainer>()
            .WithDatabase(configuration)
            .Build();

        await _container.StartAsync();

        using (NpgsqlConnection connection = new(_container.ConnectionString))
        {
            await connection.OpenAsync();
            string script = await File.ReadAllTextAsync(@"Sql/PgDoc.Core.sql");
            NpgsqlCommand command = connection.CreateCommand();
            command.CommandText = script;
            await command.ExecuteNonQueryAsync();
            connection.ReloadTypes();
        }
    }

    public async Task Reset()
    {
        using (NpgsqlConnection connection = new(_container.ConnectionString))
        {
            await connection.OpenAsync();
            NpgsqlCommand command = connection.CreateCommand();
            command.CommandText = @"TRUNCATE TABLE document;";
            await command.ExecuteNonQueryAsync();
        }
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}
