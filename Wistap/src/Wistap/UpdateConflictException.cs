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
        public UpdateConflictException(Guid id, ByteString version)
            : base($"The object '{id}' has been modified.")
        {
            this.Id = id;
            this.Version = version;
        }

        /// <summary>
        /// Gets the failed record mutation.
        /// </summary>
        public Guid Id { get; }

        public ByteString Version { get; }
    }
}
