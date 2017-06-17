
package org.apache.hadoop.rdd.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.lang.reflect.Constructor;


@SuppressWarnings("unchecked")
public class ContextFactory {

  private static final ContextFactory instance = new ContextFactory();

  public static TaskAttemptContext create(Configuration conf, TaskAttemptID taskAttemptId) {
    return instance.createInternal(conf, taskAttemptId);
  }

  private Constructor taskAttemptConstructor;

  private ContextFactory() {
    Class implClass = TaskAttemptContext.class;
    try {
      implClass = Class.forName("org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    try {
      this.taskAttemptConstructor = implClass.getConstructor(Configuration.class, TaskAttemptID.class);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private TaskAttemptContext createInternal(Configuration conf, TaskAttemptID taskAttemptId) {
    try {
      return (TaskAttemptContext) taskAttemptConstructor.newInstance(conf, taskAttemptId);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }
}
