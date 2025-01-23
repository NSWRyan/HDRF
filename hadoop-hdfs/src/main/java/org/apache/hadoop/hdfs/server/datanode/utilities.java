package org.apache.hadoop.hdfs.server.datanode;
import java.io.File;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.List;

import nayuki.nativehash.BlockHasher;
import nayuki.nativehash.Sha1;
import nayuki.nativehash.Sha224;
import nayuki.nativehash.Sha256;
import redis.clients.jedis.Jedis;

public class utilities {
  boolean native1=true;
  public byte[] longToBytes(long l, int length) {
    byte[] result = new byte[length];
    for (int i = length-1; i >= 0; i--) {
      result[i] = (byte) (l & 0xFF);
      l >>= 8;
    }
    return result;
  }

  public long bytesToLong(byte[] b, int length) {
    long result = 0;
    for (int i = 0; i < length; i++) {
      result <<= 8;
      result |= (b[i] & 0xFF);
    }
    b=null;
    return result;
  }
  
  public long bytesToBlockID(byte[] b, int threadID, int length) {
    /*
     * BlockID > MSB ThreadID (2 bits)| blockID (22bits)
     */
    if (b != null) {
      long result=0;
      int base = threadID*3;
      result= (b[base+0] & 0xFF) << 16 
          | (b[base+1] & 0xFF) << 8
          | (b[base+2] & 0xFF);
      return result;
    }
    // Create a new one
    return (threadID << 22);
  }
  public long bytesToBlockPos(byte[] b, int threadID, int length) {
    /*
     * BlockID > MSB ThreadID (2 bits)| blockID (22bits)
     */
    if (b != null) {
      long result=0;
      int base = threadID*3;
      result= (b[base+0] & 0xFF) << 16 
          | (b[base+1] & 0xFF) << 8
          | (b[base+2] & 0xFF);
      return result;
    }
    // Create a new one
    return (0);
  }
  public byte[] blockIDtoBytes(long[] lastBlockID){
    byte[] toReturn=new byte[24];
    for(int i=0;i<lastBlockID.length;i++){
      int base = i*3;
      toReturn[base+0]=(byte)(lastBlockID[i] >> 16);
      toReturn[base+1]=(byte)(lastBlockID[i] >> 8);
      toReturn[base+2]=(byte)(lastBlockID[i]);
    }
    return toReturn;
  }

  public byte[] hexStringToByteArray(String s) {
    int len = s.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
          + Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }

  String byteToHex(final byte[] hash) {
    Formatter formatter = new Formatter();
    for (byte b : hash) {
      formatter.format("%02x", b);

    }
    String result = formatter.toString();
    formatter.close();
    return result;
  }

  public byte[] sha1hash(byte[] input) {
    if(native1){
    BlockHasher hashNative = new Sha1();
    hashNative.update(input);
    return hashNative.getHash();
    }
    else{
      MessageDigest crypt;
      try {
        crypt = MessageDigest.getInstance("SHA-1");
        crypt.reset();
        crypt.update(input);
        return crypt.digest();
      } catch (NoSuchAlgorithmException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    return null;
  }
  public byte[] sha224hash(byte[] input) {
    if(native1){
    BlockHasher hashNative = new Sha224();
    hashNative.update(input);
    return hashNative.getHash();
    }
    else{
      MessageDigest crypt;
      try {
        crypt = MessageDigest.getInstance("SHA-224");
        crypt.reset();
        crypt.update(input);
        return crypt.digest();
      } catch (NoSuchAlgorithmException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    return null;
  }
  
  public void listf1(String directoryName, List<Long> blockIDs) {
    File directory = new File(directoryName);

    // Get all files from a directory.
    File[] fList = directory.listFiles();
    if(fList != null)
        for (File file : fList) {      
            if (file.isFile()) {
              if(file.length()==0&&
                  file.getName().substring(0,3).equals("blk"))
                  blockIDs.add(Long.parseLong(file.getName().substring(4,14)));
            } else if (file.isDirectory()) {
                listf1(file.getAbsolutePath(), blockIDs);
            }
        }
   }
}
