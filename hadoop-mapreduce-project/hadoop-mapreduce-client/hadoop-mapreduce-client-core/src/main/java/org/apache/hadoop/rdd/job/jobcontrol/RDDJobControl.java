
package org.apache.hadoop.rdd.job.jobcontrol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;


public class RDDJobControl implements Runnable {


  public enum ThreadState {
    RUNNING, SUSPENDED, STOPPED, STOPPING, READY
  }

    private ThreadState runnerState;

  private Map<String, RDDControlledJob> waitingJobs;
  private Map<String, RDDControlledJob> readyJobs;
  private Map<String, RDDControlledJob> runningJobs;
  private Map<String, RDDControlledJob> successfulJobs;
  private Map<String, RDDControlledJob> failedJobs;

  private long nextJobID;
  private String groupName;
  private int jobPollInterval;


  public RDDJobControl(String groupName) {
    this.waitingJobs = new Hashtable<String, RDDControlledJob>();
    this.readyJobs = new Hashtable<String, RDDControlledJob>();
    this.runningJobs = new Hashtable<String, RDDControlledJob>();
    this.successfulJobs = new Hashtable<String, RDDControlledJob>();
    this.failedJobs = new Hashtable<String, RDDControlledJob>();
    this.nextJobID = -1;
    this.groupName = groupName;
    this.runnerState = ThreadState.READY;
    this.jobPollInterval = 500;
  }

  private static List<RDDControlledJob> toList(Map<String, RDDControlledJob> jobs) {
    ArrayList<RDDControlledJob> retv = new ArrayList<RDDControlledJob>();
    synchronized (jobs) {
      for (RDDControlledJob job : jobs.values()) {
        retv.add(job);
      }
    }
    return retv;
  }



  public List<RDDControlledJob> getSuccessfulJobList() {
    return toList(this.successfulJobs);
  }

  public List<RDDControlledJob> getFailedJobList() {
    return toList(this.failedJobs);
  }

  private String getNextJobID() {
    nextJobID += 1;
    return this.groupName + this.nextJobID;
  }

  private static void addToQueue(RDDControlledJob aJob,
      Map<String, RDDControlledJob> queue) {
    synchronized (queue) {
      queue.put(aJob.getJobID(), aJob);
    }
  }

  private void addToQueue(RDDControlledJob aJob) {
    Map<String, RDDControlledJob> queue = getQueue(aJob.getJobState());
    addToQueue(aJob, queue);
  }

  private Map<String, RDDControlledJob> getQueue(RDDControlledJob.State state) {
    Map<String, RDDControlledJob> retv = null;
    if (state == RDDControlledJob.State.WAITING) {
      retv = this.waitingJobs;
    } else if (state == RDDControlledJob.State.READY) {
      retv = this.readyJobs;
    } else if (state == RDDControlledJob.State.RUNNING) {
      retv = this.runningJobs;
    } else if (state == RDDControlledJob.State.SUCCESS) {
      retv = this.successfulJobs;
    } else if (state == RDDControlledJob.State.FAILED || state == RDDControlledJob.State.DEPENDENT_FAILED) {
      retv = this.failedJobs;
    }
    return retv;
  }


  synchronized public String addJob(RDDControlledJob aJob) {
    String id = this.getNextJobID();
    aJob.setJobID(id);
    aJob.setJobState(RDDControlledJob.State.WAITING);
    this.addToQueue(aJob);
    return id;
  }


  public void stop() {
    this.runnerState = ThreadState.STOPPING;
  }


  synchronized private void checkRunningJobs() throws IOException,
      InterruptedException {

    Map<String, RDDControlledJob> oldJobs = this.runningJobs;
    this.runningJobs = new Hashtable<>();

    for (RDDControlledJob nextJob : oldJobs.values()) {
      nextJob.checkState();
      this.addToQueue(nextJob);
    }
  }

  synchronized private void checkWaitingJobs() throws IOException,
      InterruptedException {
    Map<String, RDDControlledJob> oldJobs = this.waitingJobs;
    this.waitingJobs = new Hashtable<>();

    for (RDDControlledJob nextJob : oldJobs.values()) {
      nextJob.checkState();
      this.addToQueue(nextJob);
    }
  }

  synchronized private void startReadyJobs() {
    Map<String, RDDControlledJob> oldJobs = this.readyJobs;
    this.readyJobs = new Hashtable<>();

    for (RDDControlledJob nextJob : oldJobs.values()) {

      nextJob.submit();
      this.addToQueue(nextJob);
    }
  }

  synchronized public boolean allFinished() {
    return this.waitingJobs.size() == 0 && this.readyJobs.size() == 0
        && this.runningJobs.size() == 0;
  }

  public void run() {
    this.runnerState = ThreadState.RUNNING;
    while (true) {
      while (this.runnerState == ThreadState.SUSPENDED) {
        try {
          Thread.sleep(5000);
        } catch (Exception e) {

        }
      }
      try {
        checkRunningJobs();
        checkWaitingJobs();
        startReadyJobs();
      } catch (Exception e) {
        this.runnerState = ThreadState.STOPPED;
      }
      if (this.runnerState != ThreadState.RUNNING
          && this.runnerState != ThreadState.SUSPENDED) {
        break;
      }
      try {
        Thread.sleep(jobPollInterval);
      } catch (Exception e) {

      }
      if (this.runnerState != ThreadState.RUNNING
          && this.runnerState != ThreadState.SUSPENDED) {
        break;
      }
    }
    this.runnerState = ThreadState.STOPPED;
  }
  


}
