package org.apache.fluo.core.worker.finder.hash;

import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.Notification;
import org.apache.fluo.core.worker.NotificationFinder;
import org.apache.fluo.core.worker.NotificationProcessor;
import org.apache.fluo.core.worker.TxResult;

public class PartitionNotificationFinder  implements NotificationFinder {

  private ParitionManager paritionManager;
  private Thread scanThread;
  private NotificationProcessor processor;
  private Environment env;
  private AtomicBoolean stopped;
  
  @Override
  public void init(Environment env, NotificationProcessor processor) {
    // TODO Auto-generated method stub
    this.processor = processor;
    this.env = env;
    this.stopped = new AtomicBoolean(false);
    
  }

  @Override
  public void start() {
    paritionManager = new ParitionManager(env.getSharedResources().getCurator(), processor);
    
    scanThread = new Thread(new ScanTask(this, processor, paritionManager, env, stopped));
    scanThread.setName(getClass().getSimpleName() + " " + ScanTask.class.getSimpleName());
    scanThread.setDaemon(true);
    scanThread.start();
  }

  @Override
  public void stop() {
    stopped.set(true);
    
    scanThread.interrupt();
    try {
      scanThread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    
    paritionManager.stop();
  }

  @Override
  public boolean shouldProcess(Notification notification) {
    return paritionManager.shouldProcess(notification);
  }

  @Override
  public void failedToProcess(Notification notification, TxResult status) {
    // TODO Auto-generated method stub
    
  }

}
