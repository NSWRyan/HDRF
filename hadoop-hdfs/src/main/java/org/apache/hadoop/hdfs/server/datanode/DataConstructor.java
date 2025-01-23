package org.apache.hadoop.hdfs.server.datanode;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.kerby.asn1.util.IOUtil;
import org.slf4j.Logger;
import org.xerial.snappy.Snappy;

import com.hadoop.compression.lzo.LzopCodec;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

public class DataConstructor {
  byte[] data = null;
  /*
   * Constructor class. Construct a file based on Table #1 information (Redis)
   * Construct the file with the chunks in Table #2 (RocksDB)
   * 
   * Table #1 Table #1 Block (4 bytes) | FileSize (4 bytes) | ChunksHash (num of chunks*20 bytes)
   * 
   * Table #2 ChunkHash|nCopy|blockID|blockOffsetStart|Stop > 
   * SHA1 20 bytes | 2 bytes | 3 bytes | 3 bytes | 3 bytes chunks*20 bytes)
   * 31 bytes.
   * SHA256 32 bytes | 2 bytes | 3 bytes | 3 bytes | 3 bytes chunks*32 bytes)
   * 43 bytes.
   * 
   */
  static String src;
  utilities ut1 = new utilities();
  int filesize;

  static final int nThread=2;
  Logger LOG = DataNode.LOG;
  long lastBlockID[];
  long blkID;
  boolean nonDeduplicated=true;
  int thisAIReadQueue;
  static boolean runlog = DataNode.runlog;
  int hash_length;
  
  public DataConstructor(long blkID, byte[] t1data) {
    DataConstructor.src = DataNode.chunkDir;
    if (runlog) {
      LOG.info("Rebuilding block " + blkID + " ...");
    }
    this.blkID = blkID;
    this.hash_length = DataNode.hash_length;
    // Will be "long" in the final release.
    JedisPool jPool = new JedisPool("localhost");
    Jedis jedis = jPool.getResource();

    byte[] blockID = jedis.get("blockID".getBytes());

    lastBlockID = new long[8];
    for (int i = 0; i < 4; i++) {
      lastBlockID[i] = ut1.bytesToBlockID(blockID, i, 3);
      lastBlockID[i + 4] = ut1.bytesToBlockPos(blockID, i + 4, 3);
    }
    if (t1data != null) {
      nonDeduplicated = false;

      // Divide from a long byte[] to 20 byte[] (SHA1);
      // Divide from a long byte[] to 28 byte[] (SHA2);
      byte[] buffer;
      byte[] fsize = new byte[4];
      System.arraycopy(t1data, 0, fsize, 0, 4);

      this.filesize = (int) ut1.bytesToLong(fsize, 4);

      if (DataNode.compressor == 0) {
        File compressedblock = new File(DataNode.chunkDir + blkID);
        try {
          ByteBuffer bBuffer;
          SnappyCodec codec = new SnappyCodec();
          Configuration asd = new Configuration();
          codec.setConf(asd);
          FileInputStream fis = new FileInputStream(compressedblock);
          InputStream inputStream = codec.createInputStream(fis);
          bBuffer = ByteBuffer.wrap(new byte[(int) filesize]);
          int n;
          buffer = new byte[2048];
          while ((n = inputStream.read(buffer)) >= 0) {
            bBuffer.put(buffer, 0, n);
          }
          data = bBuffer.array();
          inputStream.close();
          fis.close();
          buffer = null;
          bBuffer = null;
          asd = null;
          codec = null;
        } catch (Exception e) {
          LOG.info("Rebuilding block " + blkID + " error " + e);
          LOG.info("Decompressor -1 " + e.getMessage());
          e.printStackTrace();
        }
        /*
         * try { ByteBuffer bBuffer; RandomAccessFile randomAccessFile = new
         * RandomAccessFile(compressedblock, "r"); FileChannel fChannel =
         * randomAccessFile.getChannel(); bBuffer = ByteBuffer.wrap(new
         * byte[(int) compressedblock.length()]); fChannel.position(0);
         * fChannel.read(bBuffer); while (bBuffer.hasRemaining()) {
         * bBuffer.get(); } fChannel.close(); randomAccessFile.close(); data =
         * Snappy.uncompress(bBuffer.array()); } catch (Exception e) {
         * LOG.info("Rebuilding block " + blkID + " error " + e);
         * LOG.info("Decompression error"); e.printStackTrace(); }
         */
      } else if (DataNode.compressor == 3) {
        File compressedblock = new File(DataNode.chunkDir + blkID);
        try {
          ByteBuffer bBuffer;
          LzopCodec codec = new LzopCodec();
          Configuration asd = new Configuration();
          codec.setConf(asd);
          FileInputStream fis = new FileInputStream(compressedblock);
          InputStream inputStream = codec.createInputStream(fis);
          bBuffer = ByteBuffer.wrap(new byte[(int) filesize]);
          int n;
          buffer = new byte[2048];
          while ((n = inputStream.read(buffer)) >= 0) {
            bBuffer.put(buffer, 0, n);
          }
          data = bBuffer.array();
          inputStream.close();
          fis.close();
          buffer = null;
          bBuffer = null;
          asd = null;
          codec = null;
        } catch (Exception e) {
          LOG.info("Rebuilding block " + blkID + " error " + e);
          LOG.info("Decompressor -1 " + e.getMessage());
          e.printStackTrace();
        }
      } else if (DataNode.compressor == 4) {
        File compressedblock = new File(DataNode.chunkDir + blkID);
        try {
          ByteBuffer bBuffer;
          Lz4Codec codec = new Lz4Codec();
          Configuration asd = new Configuration();
          codec.setConf(asd);
          FileInputStream fis = new FileInputStream(compressedblock);
          InputStream inputStream = codec.createInputStream(fis);
          bBuffer = ByteBuffer.wrap(new byte[(int) filesize]);
          int n;
          buffer = new byte[2048];
          while ((n = inputStream.read(buffer)) >= 0) {
            bBuffer.put(buffer, 0, n);
          }
          data = bBuffer.array();
          inputStream.close();
          fis.close();
          buffer = null;
          bBuffer = null;
          asd = null;
          codec = null;
        } catch (Exception e) {
          LOG.info("Rebuilding block " + blkID + " error " + e);
          LOG.info("Decompressor -1 " + e.getMessage());
          e.printStackTrace();
        }
      } else if (DataNode.compressor == 5) {
        File compressedblock = new File(DataNode.chunkDir + blkID);
        try {
          ByteBuffer bBuffer;
          GzipCodec codec = new GzipCodec();
          Configuration asd = new Configuration();
          codec.setConf(asd);
          FileInputStream fis = new FileInputStream(compressedblock);
          InputStream inputStream = codec.createInputStream(fis);
          bBuffer = ByteBuffer.wrap(new byte[(int) filesize]);
          int n;
          buffer = new byte[2048];
          while ((n = inputStream.read(buffer)) >= 0) {
            bBuffer.put(buffer, 0, n);
          }
          data = bBuffer.array();
          inputStream.close();
          fis.close();
          buffer = null;
          bBuffer = null;
          asd = null;
          codec = null;
        } catch (Exception e) {
          LOG.info("Rebuilding block " + blkID + " error " + e);
          LOG.info("Decompressor -1 " + e.getMessage());
          e.printStackTrace();
        }
      } else {
        List<byte[]> hashes = new ArrayList<byte[]>();
        int pos = 4;
        for (int i = 0; i < t1data.length / hash_length; i++) {
          buffer = new byte[hash_length];
          System.arraycopy(t1data, pos + i * hash_length, buffer, 0,
              hash_length);
          hashes.add(buffer);
        }
        data = new byte[filesize];

        if (runlog) {
          LOG.info("Rebuilding block QB" + blkID + " ...");
        }
        quickBuildMT(hashes, data, jPool);
        hashes = null;

        if (runlog) {
          LOG.info("Rebuilding block " + blkID + " done.");
        }
      }
      jedis.close();
      jPool.close();
      buffer = null;
    }
    t1data = null;
    jedis = null;
    jPool = null;
    System.gc();
  }

  public void quickBuild(List<byte[]> hashes, ByteBuffer data,
      JedisPool jPool, boolean compress) {
    Jedis jedis = jPool.getResource();
    Pipeline pJedis = jedis.pipelined();
    List<chunkMeta> chunkMetaData = new ArrayList<chunkMeta>();
    HashMap<Long, List<chunkMeta>> blockMapped =
        new HashMap<Long, List<chunkMeta>>();
    HashMap<byte[], byte[]> hashToData = new HashMap<byte[], byte[]>();

    // Get the metadata from Redis
    for (byte[] hash : hashes) {
      chunkMetaData.add(new chunkMeta(pJedis.get(hash), hash, 0, 0));
    }
    pJedis.sync();

    // Iterate through the chunkMetaData and partition it based on the blockID
    for (chunkMeta chunk : chunkMetaData) {
      chunk.process(-1);
      try {
        blockMapped.get(chunk.blockID).add(chunk);
      } catch (Exception e) {
        blockMapped.put(chunk.blockID, new ArrayList<chunkMeta>());
        blockMapped.get(chunk.blockID).add(chunk);
      }
    }
    Iterator it = blockMapped.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry pair = (Map.Entry) it.next();
      File bufferFile = new File(src + pair.getKey());

      // Open the file
      try (FileChannel fChannel =
          new RandomAccessFile(bufferFile, "r").getChannel()) {
        // File new file(getKey)
        /*Read randomly
        for (chunkMeta chunk : (List<chunkMeta>) pair.getValue()) {
          if (!hashToData.containsKey(chunk.chunkHash)) {
            // Read at chunk position and put it in the chunk hash map
            ByteBuffer bBuffer = ByteBuffer.wrap(new byte[chunk.length]);
            fChannel.read(bBuffer);
            while (bBuffer.hasRemaining()) {
              bBuffer.get();
            }
            hashToData.put(chunk.chunkHash, bBuffer.array());
          }
        }
        */
        if(compress){
          ByteBuffer bBuffer = ByteBuffer.wrap(new byte[(int) bufferFile.length()]);
          fChannel.read(bBuffer);
          while (bBuffer.hasRemaining()) {
            bBuffer.get();
          }
          fChannel.close();
          byte flag=bBuffer.get(0);
          if(flag==(byte)1){
            //Uncompress the data first
            byte[] compressedData=new byte[bBuffer.capacity()-1];
            bBuffer.position(1);
            bBuffer.get(compressedData, 0, compressedData.length);
            byte[] uncompressedData=Snappy.uncompress(compressedData);
            bBuffer=null;
            bBuffer=ByteBuffer.wrap(uncompressedData);
          }

          // File new file(getKey)
          for (chunkMeta chunk : (List<chunkMeta>) pair.getValue()) {
            if (!hashToData.containsKey(chunk.chunkHash)) {
              // Read at chunk position and put it in the chunk hash map
              byte[] temp=new byte[chunk.length];
              bBuffer.position((int)chunk.blockStart);
              bBuffer.get(temp, 0, chunk.length);
              hashToData.put(chunk.chunkHash, temp);
            }
          }
        }else{
          //No compression
          ByteBuffer bBuffer = ByteBuffer.wrap(new byte[(int) bufferFile.length()]);
          fChannel.position(0);
          fChannel.read(bBuffer);
          while (bBuffer.hasRemaining()) {
            bBuffer.get();
          }
          for (chunkMeta chunk : (List<chunkMeta>) pair.getValue()) {
            if (!hashToData.containsKey(chunk.chunkHash)) {
              // Read at chunk position and put it in the chunk hash map
              byte[] temp=new byte[chunk.length];
              bBuffer.position((int) chunk.blockStart);
              bBuffer.get(temp, 0, chunk.length);
              hashToData.put(chunk.chunkHash, temp);
            }
          }
        }
      } catch (IOException x) {
        LOG.info("I/O Exception: " + x);
      }

      it.remove(); // avoids a ConcurrentModificationException
    }
    // Now rebuild it into data

    data.position(0);
    for (byte[] hash : hashes) {
      data.put(hashToData.get(hash));
    }
    jedis.close();
  }

  public void quickBuildMT(List<byte[]> hashes, byte[] data,
      JedisPool jPool) {
    Jedis jedis = jPool.getResource();
    Pipeline pJedis = jedis.pipelined();
    List<chunkMeta> chunkMetaData = new ArrayList<chunkMeta>();
    HashMap<Long, List<chunkMeta>> blockMapped =
        new HashMap<Long, List<chunkMeta>>();

    // Get the metadata from Redis
    for (byte[] hash : hashes) {
      chunkMetaData.add(new chunkMeta(pJedis.get(hash), hash, 0, 0));
    }
    pJedis.sync();
    
    // Iterate through the chunkMetaData and partition it based on the blockID
    int bbStart=0;
    int ix=0;
    for (chunkMeta chunk : chunkMetaData) {
      bbStart=chunk.process(bbStart);
      try {
        blockMapped.get(chunk.blockID).add(chunk);
        ix++;
      } catch (Exception e) {
        blockMapped.put(chunk.blockID, new ArrayList<chunkMeta>());
        blockMapped.get(chunk.blockID).add(chunk);
        ix++;
      }
    }
    chunkMetaData=null;
    List<chunkData> blockChunk=new ArrayList<chunkData>();
    Iterator it = blockMapped.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry pair = (Map.Entry) it.next();
      blockChunk.add(new chunkData((long)pair.getKey(), (List<chunkMeta>)pair.getValue()));
      it.remove();
    }
    int nThread = 1;
    List<CountDownLatch> latches = new ArrayList<CountDownLatch>();
    if(blockChunk.size()<2){
      nThread=blockChunk.size();
    }else{
      nThread=this.nThread;
    }
    latches.add(new CountDownLatch(1));
    new threadedConstructor(blockChunk, data, 
        jPool, 0, nThread, latches, lastBlockID, blkID).start();
    try {
      latches.get(0).await();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }finally{
      jedis.close();
      blockMapped=null;
      blockChunk=null;
      latches=null;
    }
  }
  
}

class chunkData {
  long blockID;
  List<chunkMeta> metaData;
  chunkData(long blockID, List<chunkMeta> metaData){
    this.blockID=blockID;
    this.metaData=metaData;
  }
}

class threadedConstructor implements Runnable {
  private Thread t;
  public List<chunkData> blockChunk;
  public List<chunkMeta> toStore;
  public boolean done = false;
  public byte[] data;
  public long blkID;
  int storeSize;
  long[] lastBlockID;
  private int nThis;
  private int nThread;
  private int start;
  private int stop;
  Pipeline pJedis;
  utilities ut1 = new utilities();
  public List<CountDownLatch> latches;
  Logger LOG = DataNode.LOG;
  int compressor;
  int maxSize;
  
  threadedConstructor(
      List<chunkData> blockChunk, byte[] data,
      JedisPool jPool, int nThis, int nThread,
      List<CountDownLatch> latches, long[] lastBlockID, long blkID) {
    this.blockChunk = blockChunk;
    this.start = blockChunk.size() * nThis / nThread;
    this.stop = blockChunk.size() * (nThis + 1) / nThread;
    this.nThis = nThis;
    this.blkID = blkID;
    this.nThread = nThread;
    this.latches = latches;
    this.lastBlockID=lastBlockID;
    this.data = data;
    this.maxSize=DataDeduplicator.maxSize;
    latches.add(new CountDownLatch(1));
    if (nThis != (nThread - 1)) {
      // Execute new thread, recursive?
      new threadedConstructor(blockChunk, data, jPool, nThis + 1,
          nThread, latches, lastBlockID, blkID).start();
    }
    this.compressor=DataNode.compressor;
  }

  @Override
  public void run() {
    if(DataConstructor.runlog){LOG.info("Rebuilding block "+nThis +" ...");}
    for (int i = stop-1; i >= start; i--) {
      if(DataConstructor.runlog){LOG.info("Rebuilding block "+nThis +" ..."+i);}
      File bufferFile = new File(DataConstructor.src + blockChunk.get(i).blockID);
      ByteBuffer bBuffer;
      try  {
          boolean uncompressQ=true;
          if(compressor==2){
            for(int i2=0;i2<DataDeduplicator.nThread;i2++){
              if(lastBlockID[i2]==blockChunk.get(i).blockID){
                uncompressQ=false;
              }
            }
          }else{
            uncompressQ=false;
          }
          
          if (uncompressQ) {
            // Uncompress the data first
            if(DataConstructor.runlog){LOG.info("Rebuilding block "+nThis +" ..."+i+"uncompressed");}
            Lz4Codec codec = new Lz4Codec();
            Configuration asd = new Configuration();
            codec.setConf(asd);
            FileInputStream fis = new FileInputStream(bufferFile);
            InputStream inputStream = codec.createInputStream(
                fis);
            bBuffer = ByteBuffer.wrap(new byte[maxSize]);
            int n;
            byte[] buffer = new byte[2048];
            while ((n = inputStream.read(buffer)) >= 0) {
              bBuffer.put(buffer, 0, n);
            }
            inputStream.close();
            fis.close();
            buffer=null;
            asd=null;
            codec=null;
          } else{
            RandomAccessFile randomAccessFile = new RandomAccessFile(bufferFile, "r");
            FileChannel fChannel = randomAccessFile.getChannel();
            bBuffer =
                ByteBuffer.wrap(new byte[(int) bufferFile.length()]);
              if(DataConstructor.runlog){LOG.info("Rebuilding block "+nThis +" ..."+i+"compress");}
              fChannel.position(0);
              fChannel.read(bBuffer);
              while (bBuffer.hasRemaining()) {
                bBuffer.get();
              }
            fChannel.close();
            randomAccessFile.close();
          }
          // File new file(getKey)
        for (chunkMeta chunk : blockChunk.get(i).metaData) {
            // Read at chunk position and put it in the chunk hash map
            bBuffer.position((int) chunk.blockStart);
            bBuffer.get(data, chunk.bbStart, chunk.length);
        }
      } catch (IOException x) {
        LOG.info(""+x);
      }finally{
        bBuffer = null;
      }
    }
    
    if (nThis != 0) {
      try {
        latches.get(nThis).await();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    latches.get(nThis + 1).countDown();
    System.gc();
    if (nThis == 0) {
      try {
        latches.get(nThread).await();
        latches.get(0).countDown();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  public void start() {
    if (t == null) {
      t = new Thread(this);
      t.start();
    }
  }

}