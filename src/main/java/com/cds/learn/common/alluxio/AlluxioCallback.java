package com.cds.learn.common.alluxio;

import alluxio.client.file.FileSystem;


public interface AlluxioCallback<T> {
    T doInAlluxio(FileSystem fileSystem) throws Exception;
}
