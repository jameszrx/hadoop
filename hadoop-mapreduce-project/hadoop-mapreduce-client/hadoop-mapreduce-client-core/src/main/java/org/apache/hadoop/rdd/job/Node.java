
package org.apache.hadoop.rdd.job;

import org.apache.hadoop.rdd.func.TransformFunc;
import org.apache.hadoop.rdd.io.save.MultipleOutputSaver;
import org.apache.hadoop.rdd.io.save.OutputSaver;
import org.apache.hadoop.rdd.io.save.Saver;
import org.apache.hadoop.rdd.io.save.TempSaver;
import org.apache.hadoop.rdd.types.Converter;

import java.io.Serializable;
import java.util.List;

public class Node implements Serializable {

  private final String nodeName;
  private TransformFunc<Object, Object> func;
  private final List<Node> children;
  private final Converter inputConverter;
  private final Converter outputConverter;
  private final String outputName;

  private transient Saver<Object> saver;

  public Node(TransformFunc<Object, Object> func, String name, List<Node> children, Converter inputConverter,
              Converter outputConverter, String outputName) {
    this.func = func;
    this.nodeName = name;
    this.children = children;
    this.inputConverter = inputConverter;
    this.outputConverter = outputConverter;
    this.outputName = outputName;
  }

  public void initialize(RDDTaskContext ctxt) {
    func.setContext(ctxt.getContext());
    for (Node child : children) {
      child.initialize(ctxt);
    }
    if (outputConverter != null) {
      if (outputName != null) {
        this.saver = new MultipleOutputSaver(outputConverter, ctxt.getMultipleOutputs(), outputName);
      } else {
        this.saver = new OutputSaver(outputConverter, ctxt.getContext());
      }
    } else {
      this.saver = new TempSaver(children);
    }
  }


  public void process(Object input) {
      func.process(input, saver);
  }

  public void process(Object key, Object value) {
    process(inputConverter.convertInput(key, value));
  }

  public void processIterable(Object key, Iterable values) {
    process(inputConverter.convertIterableInput(key, values));
  }

  public void cleanup() {
    func.cleanup(saver);
    for (Node child : children) {
      child.cleanup();
    }
  }


}
