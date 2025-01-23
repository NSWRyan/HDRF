package org.apache.hadoop.hdfs.server.datanode;

import org.slf4j.Logger;

import redis.clients.jedis.Jedis;

public class preBuildDC implements Runnable {
  private Thread t;
  long blockID;
  Logger LOG = DataNode.LOG;
  
  public preBuildDC(long blockID){
    this.blockID=blockID;
  }
  @Override
  public void run() {
    // TODO Auto-generated method stub
    DataConstructor dConstructor;
    utilities ut1=new utilities();
    int indexInBlockIDs=DataNode.blockIDs.indexOf(new Long(blockID));
    if(indexInBlockIDs!=-1&&
        indexInBlockIDs!=(DataNode.blockIDs.size()-1)){
      byte[] t1data;
      Jedis jedis = new Jedis("localhost");
      
      blockID=DataNode.blockIDs.get(indexInBlockIDs+1);
      LOG.info("preBuild rebuild "+blockID);
      DataNode.preBuildDataDone=false;
      t1data = jedis.get(ut1.longToBytes(blockID, 4));
      if(t1data!=null){
        DataNode.preBuildDataBlockID=blockID;
        dConstructor=new DataConstructor(blockID,t1data);
        DataNode.preBuildData=dConstructor.data;
        DataNode.preBuildDataDone=true;
        LOG.info("preBuildDC done "+blockID);
      }else{
        DataNode.preBuildData=null;
        DataNode.preBuildDataBlockID=0;
        LOG.info("preBuildDC nonDDblock"+blockID);
      }
      t1data=null;
      jedis.close();
      jedis=null;
    }
    dConstructor = null;
    //dConstructor.data=null;
    //DataNode.preBuildCoolDown=System.currentTimeMillis()+180000;
    System.gc();
  }
  
  public void start() {
    if (t == null) {
      t = new Thread(this);
      t.start();
    }
  }
}
