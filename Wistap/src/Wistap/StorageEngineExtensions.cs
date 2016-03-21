﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Wistap
{
    public static class StorageEngineExtensions
    {
        public static Task<ByteString> UpdateObjects(this IStorageEngine storageEngine, ByteString account, params DataObject[] objects)
        {
            return storageEngine.UpdateObjects(account, (IEnumerable<DataObject>)objects, (IEnumerable<DataObject>)new DataObject[0]);
        }

        public static Task<ByteString> UpdateObject(this IStorageEngine storageEngine, ByteString account, ObjectId id, string payload, ByteString version)
        {
            return storageEngine.UpdateObjects(account, new DataObject(id, payload, version));
        }
    }
}
