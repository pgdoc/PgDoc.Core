using System;

namespace Wistap
{
    /// <summary>
    /// Represents an error caused by the attempt of modifying a record using the wrong base version.
    /// </summary>
    public class UpdateConflictException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ConcurrentMutationException"/> class.
        /// </summary>
        /// <param name="failedUpdate">The failed record mutation.</param>
        public UpdateConflictException(ByteString id, ByteString version)
            : base($"Version '{version}' of object '{id}' no longer exists.")
        {
            this.Id = id;
            this.Version = version;
        }

        /// <summary>
        /// Gets the failed record mutation.
        /// </summary>
        public ByteString Id { get; }

        public ByteString Version { get; }
    }
}
