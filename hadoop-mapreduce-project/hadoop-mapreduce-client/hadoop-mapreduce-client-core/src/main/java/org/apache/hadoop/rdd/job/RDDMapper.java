package org.apache.hadoop.rdd.job;


import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class RDDMapper extends Mapper<Object, Object, Object, Object> {

  private Node node;
  private RDDTaskContext ctxt;

  @Override
  protected void setup(Mapper<Object, Object, Object, Object>.Context context) {
    List<Node> nodes;
    this.ctxt = new RDDTaskContext(context, NodeTask.MAP);
    try {
      nodes = ctxt.getNodes();
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }
      this.node = nodes.get(0);
  }

  @Override
  protected void map(Object k, Object v, Mapper<Object, Object, Object, Object>.Context context) {
    node.process(k, v);
  }

  @Override
  protected void cleanup(Mapper<Object, Object, Object, Object>.Context context) {
    node.cleanup();
    ctxt.cleanup();
  }
}
