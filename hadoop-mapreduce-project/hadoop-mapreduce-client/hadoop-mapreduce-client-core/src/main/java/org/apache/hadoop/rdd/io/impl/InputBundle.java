
package org.apache.hadoop.rdd.io.impl;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;

import java.io.Serializable;
import java.util.Map;


public class InputBundle implements Serializable {

  private Class<? extends InputFormat> inputFormatClass;
  private Map<String, String> extraConf;

  public InputBundle(Class<? extends InputFormat> inputFormatClass) {
    this.inputFormatClass = inputFormatClass;
    this.extraConf = Maps.newHashMap();
  }

  public Class<? extends InputFormat> getInputFormatClass() {
    return inputFormatClass;
  }


  public Configuration configure(Configuration conf) {
    for (Map.Entry<String, String> e : extraConf.entrySet()) {
      conf.set(e.getKey(), e.getValue());
    }
    return conf;
  }

}
