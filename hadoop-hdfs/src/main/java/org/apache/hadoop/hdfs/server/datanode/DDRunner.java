package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;

import redis.clients.jedis.Jedis;

public class DDRunner implements Runnable{
  private Thread t;
  private ByteBuffer bf1;
  long blockID;
  Logger LOG = DataNode.LOG;
  boolean runlog = DataNode.runlog;
  
  public DDRunner(ByteBuffer bf1, long blockID){
    this.bf1=bf1;
    this.blockID=blockID;
  }
  
  @Override
  public void run() {
    try {
      DataDeduplicator a = new DataDeduplicator(bf1, blockID);
      a = null;
    } catch (Exception e) {
      LOG.info("DD crash" + e);
    }
    incrementQueue();
    bf1 = null;
    System.gc();
  }
  void incrementQueue(){
    try {
      int currentQ = DataXceiver.AIWriteTurn.get();
      if(runlog)LOG.info("br #" + DataXceiver.AIWriteTurn.get()
          + " incrementing queue.");
      while (currentQ >= DataXceiver.AIWriteTurn
          .incrementAndGet()) {
        if(runlog)LOG.info("br #" + DataXceiver.AIWriteTurn.get()
            + " incrementing queue.");
        Thread.sleep(10);
      }
    } catch (Exception e) {
      LOG.info(
          "BlockReceiver DataXceiver queue can't resolve queue"
              + e);
    }
  }

  public void start() {
    if (t == null) {
      t = new Thread(this);
      t.start();
    }
  }
}
