
package org.apache.hadoop.rdd.plan;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.util.List;


public class JobNameBuilder {

  private static final Joiner JOINER = Joiner.on("+");
  private static final Joiner CHILD_JOINER = Joiner.on("/");

  private String streamName;
  List<String> rootStack = Lists.newArrayList();

  public JobNameBuilder(final String streamName) {
    this.streamName = streamName;
  }

  public void visit(TransformNode node) {
    visit(node, rootStack);
  }

  public void visit(List<TransformNode> nodes) {
    visit(nodes, rootStack);
  }

  private void visit(List<TransformNode> nodes, List<String> stack) {
    if (nodes.size() == 1) {
      visit(nodes.get(0), stack);
    } else {
      List<String> childStack = Lists.newArrayList();
      for (int i = 0; i < nodes.size(); i++) {
        TransformNode node = nodes.get(i);
        List<String> subStack = Lists.newArrayList();
        visit(node, subStack);
        if (!subStack.isEmpty()) {
          childStack.add("[" + JOINER.join(subStack) + "]");
        }
      }
      if (!childStack.isEmpty()) {
        stack.add("[" + CHILD_JOINER.join(childStack) + "]");
      }
    }
  }

  private void visit(TransformNode node, List<String> stack) {
    String name = node.getName();
    if (!name.isEmpty()) {
      stack.add(node.getName());
    }
    visit(node.getChildren(), stack);
  }

  public String build() {
    return String.format("%s: %s", streamName, JOINER.join(rootStack));
  }
}
