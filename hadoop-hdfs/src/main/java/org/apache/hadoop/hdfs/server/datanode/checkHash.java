package org.apache.hadoop.hdfs.server.datanode;
/*
 * A class for hash checking
 */

public class checkHash {
  private byte[] hash = null;
  private int pos = -1;

  checkHash(byte[] hash, int pos){
    this.hash=hash;
    this.pos=pos;
  }
  
  public byte[] getHash() {
    return (hash);
  }
  public int getPos(){
    return(pos);
  }
}
