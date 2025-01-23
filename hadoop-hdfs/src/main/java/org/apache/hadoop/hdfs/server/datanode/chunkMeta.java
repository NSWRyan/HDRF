package org.apache.hadoop.hdfs.server.datanode;
import java.util.concurrent.ExecutionException;

import redis.clients.jedis.Response;

//Metadata for chunks
public class chunkMeta {
  boolean newChunk=true;
  byte[] chunkHash;
  int nCopy;
  long blockID;
  int blockStart;
  int blockStop;
  int bbStart;
  int bbStop;
  int length;
  Response<byte[]> response;
  /*
   * nCopy = number of copy (1 bytes). 
   * blockID = chunk location (3 bytes) MAX_BLOCK_SIZE 16 MB. 
   * blockID => 2 bit threadID, 22 bitID, max/thread 4 mil blocks. 
   * blockOffset = offset in block (3 bytes) 16 MB max. 
   * Max capacity 70 TB. 
   * nCopy|blockID|blockOffset
   */
  chunkMeta(Response<byte[]> response, byte[] chunkHash, int bbStart,
      int bbStop) {
    this.bbStart = bbStart;
    this.bbStop = bbStop;
    this.length=(int) (bbStop-bbStart);
    this.chunkHash = chunkHash;
    this.response = response;
  }
  
  public int process(int bbStart){
    byte[] metaData;
    metaData = response.get();
    if (metaData == null) {
      newChunk = true;
      nCopy = 1;
      return this.bbStart;
    } else {
      // ExtractMetaData
      newChunk = false;
      nCopy = (metaData[0] & 0xFF);
      blockID = (metaData[1] & 0xFF) << 16 | (metaData[2] & 0xFF) << 8
          | (metaData[3] & 0xFF);
      //Metadata[11] XXXX0000    0000YYYY x=stary y =stop
      blockStart =(metaData[10] & 0xF0) << 20 | (metaData[4] & 0xFF) << 16
          | (metaData[5] & 0xFF) << 8 | (metaData[6] & 0xFF);
      blockStop = (metaData[10] & 0x0F) << 24 | (metaData[7] & 0xFF) << 16
          | (metaData[8] & 0xFF) << 8 | (metaData[9] & 0xFF);
      this.length = (int) (blockStop - blockStart);
      if(bbStart!=-1){
        this.bbStart=bbStart;
      }
      nCopy++;
      return this.bbStart+length;
    }
  }

  public byte[] getMeta() {
    byte[] temp = new byte[11];
    temp[0] = (byte) (nCopy);
    temp[1]=(byte)(blockID >> 16);
    temp[2]=(byte)(blockID >> 8);
    temp[3]=(byte)(blockID);
    temp[4]=(byte)(blockStart >> 16);
    temp[5]=(byte)(blockStart >> 8);
    temp[6]=(byte)(blockStart);
    temp[7]=(byte)(blockStop >> 16);
    temp[8]=(byte)(blockStop >> 8);
    temp[9]=(byte) (blockStop);
    //XXXX0000    0000YYYY
    temp[10] =(byte)( ((blockStart >> 20)&0xF0) |  ((blockStop >> 24)&0x0F));
    return temp;
  }
  
  public void setBlockStartStop(int curPos){
    blockStart=curPos;
    blockStop=curPos+length;
  }
  
  public void increment(){
    nCopy++;
  }
}