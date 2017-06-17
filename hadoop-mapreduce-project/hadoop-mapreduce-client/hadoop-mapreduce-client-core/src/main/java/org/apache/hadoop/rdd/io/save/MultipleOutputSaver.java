
package org.apache.hadoop.rdd.io.save;

import org.apache.hadoop.rdd.job.output.RDDMultipleOutputs;
import org.apache.hadoop.rdd.types.Converter;

public class MultipleOutputSaver<T, K, V> implements Saver<T> {

  private final Converter converter;
  private final RDDMultipleOutputs<K, V> outputs;
  private final String outputName;

  public MultipleOutputSaver(Converter converter, RDDMultipleOutputs<K, V> outputs, String outputName) {
    this.converter = converter;
    this.outputs = outputs;
    this.outputName = outputName;
  }

  @Override
  public void save(T saved) {
    try {
      this.outputs.write(outputName, converter.outputKey(saved), converter.outputValue(saved));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }



}
