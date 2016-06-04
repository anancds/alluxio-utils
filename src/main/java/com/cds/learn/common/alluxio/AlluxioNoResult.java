
package com.cds.learn.common.alluxio;

import alluxio.client.file.FileSystem;

public interface AlluxioNoResult {
    void doInAlluxio(FileSystem fileSystem) throws Exception;
}
