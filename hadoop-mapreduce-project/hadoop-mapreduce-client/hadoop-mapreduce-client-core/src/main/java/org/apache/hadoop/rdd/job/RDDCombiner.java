
package org.apache.hadoop.rdd.job;

public class RDDCombiner extends RDDReducer {

  @Override
  protected NodeTask getNodeContext() {
    return NodeTask.COMBINE;
  }

}
