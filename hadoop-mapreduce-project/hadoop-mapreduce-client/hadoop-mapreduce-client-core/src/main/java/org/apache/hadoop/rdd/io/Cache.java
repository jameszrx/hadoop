
package org.apache.hadoop.rdd.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;


public class Cache {


  public static void write(Configuration conf, Path path, Object value) throws IOException {
    ObjectOutputStream oos = new ObjectOutputStream(path.getFileSystem(conf).create(path));
    oos.writeObject(value);
    oos.close();
    DistributedCache.addCacheFile(path.toUri(), conf);
  }

  public static Object read(Configuration conf, Path path) throws IOException {
    URI target = null;
    for (URI uri : DistributedCache.getCacheFiles(conf)) {
      if (uri.toString().equals(path.toString())) {
        target = uri;
        break;
      }
    }
    Object value = null;
    if (target != null) {
      Path targetPath = new Path(target.toString());
      ObjectInputStream ois = new ObjectInputStream(targetPath.getFileSystem(conf).open(targetPath));
      try {
        value = ois.readObject();
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
      ois.close();
    }
    return value;
  }


}
