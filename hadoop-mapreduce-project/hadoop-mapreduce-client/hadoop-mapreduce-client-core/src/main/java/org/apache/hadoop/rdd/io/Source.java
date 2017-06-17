
package org.apache.hadoop.rdd.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.rdd.types.Type;

import java.io.IOException;


public interface Source<T> {

  Type<T> getType();


  void configureSource(Job job) throws IOException;


  long getSize(Configuration configuration);
}
