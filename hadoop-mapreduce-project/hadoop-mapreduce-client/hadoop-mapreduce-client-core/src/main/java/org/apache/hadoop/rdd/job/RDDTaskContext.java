
package org.apache.hadoop.rdd.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.rdd.io.Cache;
import org.apache.hadoop.rdd.job.output.RDDMultipleOutputs;

import java.io.IOException;
import java.util.List;

public class RDDTaskContext {

  private final TaskInputOutputContext<Object, Object, Object, Object> taskContext;
  private final NodeTask nodeTask;
  private RDDMultipleOutputs<Object, Object> multipleOutputs;

  public RDDTaskContext(TaskInputOutputContext<Object, Object, Object, Object> taskContext, NodeTask nodeTask) {
    this.taskContext = taskContext;
    this.nodeTask = nodeTask;
  }

  public TaskInputOutputContext<Object, Object, Object, Object> getContext() {
    return taskContext;
  }
  
  public List<Node> getNodes() throws IOException {
    Configuration conf = taskContext.getConfiguration();
    Path path = new Path(new Path(conf.get("RDDWorkPath")), nodeTask.toString());
    List<Node> nodes = (List<Node>) Cache.read(conf, path);
    if (nodes != null) {
      for (Node node : nodes) {
        node.initialize(this);
      }
    }
    return nodes;
  }


  public void cleanup() {
    if (multipleOutputs != null) {
      try {
        multipleOutputs.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  RDDMultipleOutputs<Object, Object> getMultipleOutputs() {
    if (multipleOutputs == null) {
      multipleOutputs = new RDDMultipleOutputs<>(taskContext);
    }
    return multipleOutputs;
  }
}
