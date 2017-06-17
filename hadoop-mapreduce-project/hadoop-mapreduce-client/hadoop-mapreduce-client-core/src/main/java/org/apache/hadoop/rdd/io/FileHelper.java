package org.apache.hadoop.rdd.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;


public class FileHelper {

  public static long getPathSize(Configuration conf, Path path) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    if (!fs.exists(path)) {
      return -1;
    }
    FileStatus[] status;

    status = fs.listStatus(path);
    if (status == null) {
      throw new FileNotFoundException(path + " doesn't exist");
    }

    long size = 0;
    for (FileStatus s : status) {
      size += s.getLen();
    }
    return size;
  }
}
