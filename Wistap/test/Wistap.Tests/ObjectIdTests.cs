using System;
using System.IO;
using Xunit;

namespace Wistap.Tests
{
    public class ObjectIdTests
    {
        [Theory]
        [InlineData((short)0)]
        [InlineData((short)1)]
        [InlineData((short)-1)]
        [InlineData(short.MaxValue)]
        [InlineData(short.MinValue)]
        [InlineData((short)255)]
        [InlineData((short)256)]
        public void Constructor_Success(short value)
        {
            DocumentId objectId = DocumentId.New(value);
            Assert.Equal(value, (short)objectId.Type);
        }
    }
}
