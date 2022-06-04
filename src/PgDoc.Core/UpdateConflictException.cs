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

namespace PgDoc.Core;

using System;

/// <summary>
/// Represents an error that occurs when attempting to modify a document using the wrong base version.
/// </summary>
public class UpdateConflictException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="UpdateConflictException"/> class.
    /// </summary>
    public UpdateConflictException(Guid id, long version)
        : base($"The object '{id}' has been modified.")
    {
        Id = id;
        Version = version;
    }

    /// <summary>
    /// Gets the failed record mutation.
    /// </summary>
    public Guid Id { get; }

    public long Version { get; }
}
