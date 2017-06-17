
package org.apache.hadoop.rdd.io.save;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.rdd.job.Node;

import java.util.List;


public class TempSaver implements Saver<Object> {

  private final List<Node> children;

  public TempSaver(List<Node> children) {
    this.children = ImmutableList.copyOf(children);
  }

  public void save(Object saved) {
    for (Node child : children) {
      child.process(saved);
    }
  }

}
