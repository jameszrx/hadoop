
package org.apache.hadoop.rdd.job;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

public class RDDReducer extends Reducer<Object, Object, Object, Object> {

  private Node node;
  private RDDTaskContext ctxt;


  protected NodeTask getNodeContext() {
    return NodeTask.REDUCE;
  }

  @Override
  protected void setup(Reducer<Object, Object, Object, Object>.Context context) {
    this.ctxt = new RDDTaskContext(context, getNodeContext());
    try {
      List<Node> nodes = ctxt.getNodes();
      this.node = nodes.get(0);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  protected void reduce(Object key, Iterable<Object> values, Reducer<Object, Object, Object, Object>.Context context) {
        node.processIterable(key, values);
  }

  @Override
  protected void cleanup(Reducer<Object, Object, Object, Object>.Context context) {
    node.cleanup();
    ctxt.cleanup();
  }
}
