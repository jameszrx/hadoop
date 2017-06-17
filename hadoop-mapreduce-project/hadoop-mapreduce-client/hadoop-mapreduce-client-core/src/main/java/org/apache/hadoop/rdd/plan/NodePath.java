
package org.apache.hadoop.rdd.plan;

import com.google.common.collect.Lists;
import org.apache.hadoop.rdd.structure.RDDListImpl;

import java.util.Iterator;
import java.util.LinkedList;

class NodePath implements Iterable<RDDListImpl<?>> {
  private LinkedList<RDDListImpl<?>> path;

  public NodePath() {
    this.path = Lists.newLinkedList();
  }

  public NodePath(RDDListImpl<?> tail) {
    this.path = Lists.newLinkedList();
    this.path.add(tail);
  }

  public NodePath(NodePath other) {
    this.path = Lists.newLinkedList(other.path);
  }

  public void push(RDDListImpl<?> stage) {
    this.path.push(stage);
  }

  public void close(RDDListImpl<?> head) {
    this.path.push(head);
  }

  public Iterator<RDDListImpl<?>> iterator() {
    return path.iterator();
  }

  public Iterator<RDDListImpl<?>> descendingIterator() {
    return path.descendingIterator();
  }

  public RDDListImpl<?> get(int index) {
    return path.get(index);
  }

  public RDDListImpl<?> head() {
    return path.peekFirst();
  }

  public RDDListImpl<?> tail() {
    return path.peekLast();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof NodePath)) {
      return false;
    }
    NodePath nodePath = (NodePath) other;
    return path.equals(nodePath.path);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (RDDListImpl<?> collect : path) {
      sb.append(collect.getName() + "|");
    }
    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  public NodePath splitAt(int splitIndex, RDDListImpl<?> newHead) {
    NodePath top = new NodePath();
    for (int i = 0; i <= splitIndex; i++) {
      top.path.add(path.get(i));
    }
    LinkedList<RDDListImpl<?>> nextPath = Lists.newLinkedList();
    nextPath.add(newHead);
    nextPath.addAll(path.subList(splitIndex + 1, path.size()));
    path = nextPath;
    return top;
  }
}