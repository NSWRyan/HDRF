package org.apache.hadoop.hdfs.server.datanode;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.xerial.snappy.Snappy;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import com.hadoop.compression.lzo.LzoCodec;
import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.compression.lzo.LzopInputStream;
import com.hadoop.compression.lzo.LzopOutputStream;

import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;

/*Data deduplicator class includes the db connection handler.
 * For beta, we are using redis for the db.
 * 2 Tables
 * Block|ChunksHash  and ChunksHash|Location
 * 
 * Table #1
 * Table #1 Block (4 bytes) | FileSize (4 bytes) | ChunksHash (num of chunks*20 bytes)
 * 
 * Table #2
 * ChunkHash|nCopy|blockID|blockOffsetStart|Stop
 *  > SHA1 20 bytes | 2 bytes | 3 bytes | 3 bytes | 3 bytes
 * 
 * Table #3 (For later when deletion
 * blockID|[hash|offset]
 * 
 * stats => structure:
 * size|duplicate (3 bytes|3 bytes)
 * 100 MB block 1kB chunk = 100k chunks
 * 1st table = 2 MB (4 + 2000000)
 * 2nd table = 100k*23B= 2.3 MB
 * Database overhead= approx. 4.3%, max 4.7 (2.4 +2.3).
 * 
 * Datadeduplicator input ByteBuffer,
 * output void,
 * 
 * init DB
 * 
 * chunk block1
 * hash chunks
 * insert db
 */
public class DataDeduplicator {
  long before;
  private final long filename;
  private final int size;
  private String dirChunks = System.getProperty("user.home") + "/testdata/";

  private final float chunksize = 1000;
  private long[] lastBlockID;
  JedisPool jPool;
  utilities ut1 = new utilities();
  boolean debug = false;
  int storeSize = 0;
  //////////////////////////////////////////////////////////////////
  static final int nThread = 3;
  static boolean compress = true;
  static int maxSize;
  // static int maxSize = (int) Math.pow(2, 26); //64 MB
  boolean runlog = DataNode.runlog;
  //boolean runlog=true;
  //////////////////////////////////////////////////////////////////
  static final Logger LOG = DataNode.LOG;
  static AtomicInteger AIWriteQueue;
  static AtomicInteger AIWriteQueueTurn;
  int thisAIWriteQueue;
  int HDFSQueue;
  int hash_length;
  // private final Connection c;
  // constructor
  public DataDeduplicator(ByteBuffer block1, long filename) {
    if (runlog) {
      LOG.info("Deduplicating block " + filename + " started.");
    }
    this.hash_length = DataNode.hash_length;
    this.filename = filename;
    this.size = block1.position();
    block1.position(0);
    // Init DBs
    // Jedis jedis = new Jedis("localhost", 16379);
    // jPool = new JedisPool("localhost",16379);
    jPool = new JedisPool("localhost");
    Jedis jedis = jPool.getResource();
    Transaction tJedis = jedis.multi();
    List<Integer> offsets = chunking(block1);
    List<chunkMeta> chunkMetaData = new ArrayList<chunkMeta>();
    if (AIWriteQueue == null) {
      AIWriteQueue = new AtomicInteger(0);
      AIWriteQueueTurn = new AtomicInteger(0);
      thisAIWriteQueue = 0;
    } else {
      try {
        thisAIWriteQueue = AIWriteQueue.incrementAndGet();
        if (runlog) {
          LOG.info("Datadeduplicator " + filename + " started at queue "
              + thisAIWriteQueue + ".");
        }
      } catch (Exception e) {
        if (runlog) {
          LOG.info("DataDeduplicator error increasing Queue " + e);
        }
        AIWriteQueue = new AtomicInteger(0);
        AIWriteQueueTurn = new AtomicInteger(0);
        thisAIWriteQueue = 0;
      }
    }
    if (runlog) {
      LOG.info("Queue for block " + filename + " started.");
    }
    while (thisAIWriteQueue > AIWriteQueueTurn.get()) {
      if (runlog) {
        LOG.info("DataDeduplicator waiting queue mine: " + thisAIWriteQueue
            + ", current turn " + AIWriteQueueTurn);
      }
      try {
        Thread.sleep(25);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        LOG.info("Sleep crash" + e.getMessage());
      }
    }

    if (runlog) {
      LOG.info("Processing for block " + filename + " queue " + thisAIWriteQueue
          + "started.");
    }

    Response<byte[]> blockID = tJedis.get("blockID".getBytes());
    tJedis.exec();

    lastBlockID = new long[8];
    for (int i = 0; i < 4; i++) {
      lastBlockID[i] = ut1.bytesToBlockID(blockID.get(), i, 3);
      lastBlockID[i + 4] = ut1.bytesToBlockPos(blockID.get(), i + 4, 3);
    }

    List<byte[]> hashes = chunkHash(jPool, chunkMetaData, block1, offsets);
    if (runlog) {
      LOG.info("Internal check for block " + filename + " started.");
    }
    checkChunk(hashes, offsets, chunkMetaData);

    System.gc();
    if (runlog) {
      LOG.info("Store process for block " + filename + " started.");
    }
    storeChunksMT(block1, chunkMetaData, jPool);

    if (runlog) {
      LOG.info("Store DB for block " + filename + " started.");
    }
    // Update DB
    storeDB(block1, hashes, jPool, filename);
    hashes = null;
    if (runlog) {
      LOG.info("AIWriteQueueTurn = " + AIWriteQueueTurn.get()
          + " AIWriteQueue = " + AIWriteQueue.get());
    }

    if (AIWriteQueueTurn.incrementAndGet() > AIWriteQueue.get()) {
      if (runlog) {
        LOG.info("Resetting Queue");
      }
      // Reset Queue
      AIWriteQueueTurn = null;
      AIWriteQueue = null;
    }
    offsets = null;
    chunkMetaData = null;
    jedis.close();
    jPool.close();
    // Cleanup
    block1 = null;
    jedis = null;
    jPool = null;
    System.gc();
    if (runlog) {
      LOG.info("Writing for block " + filename + " released.");
    }
  }

  public void debug(JedisPool jPool, List<chunkMeta> chunkMetaData) {
    Jedis jedis = jPool.getResource();
    Transaction tJedis = jedis.multi();
    List<chunkMeta> cmTempList = new ArrayList<chunkMeta>();
    for (chunkMeta chunk : chunkMetaData) {
      Response<byte[]> rA = tJedis.get(chunk.chunkHash);
      cmTempList
          .add(new chunkMeta(rA, chunk.chunkHash, chunk.bbStart, chunk.bbStop));
    }
    tJedis.exec();
    for (chunkMeta cmTemp : cmTempList) {
      cmTemp.process(-1);
      System.out.println("hash : " + ut1.byteToHex(cmTemp.chunkHash) + " nCopy "
          + cmTemp.nCopy + " Offset start " + cmTemp.blockStart
          + " Offset stop " + cmTemp.blockStop + " Block ID " + cmTemp.blockID);
    }
  }

  public int arraycompare(List<byte[]> hashes, List<byte[]> hashes2) {
    boolean i = true;
    int count = 0;
    if (hashes.size() != hashes2.size()) {
      System.out.println("assy " + hashes.size() + " and " + hashes2.size());
      return 0;
    }
    while (count < hashes2.size()) {
      if (!Arrays.equals(hashes.get(count), hashes2.get(count))) {
        i = false;
        break;
      }
      count++;
    }
    if (i) {
      System.out.println("Equal");

    } else {
      System.out.println("Not equal");
    }
    return 1;
  }

  /*
   * Process a byte buffer and return a list of offset 09/12 done
   */

  public List<Integer> chunking(ByteBuffer data) {
    data.position(0);
    int w = 700;
    int[] offsetarray = new int[data.capacity() / w + 1];
    byte mValue = data.get(0);
    int mPos = w;
    int count = -1;
    int cLength = 0;
    int mLength = 1000000;

    for (int i = 0; i < size; i++) {
      cLength++;
      if (data.get(i) >= mValue) {
        if (i > mPos) {
          count++;
          offsetarray[count] = i + 1;
          mPos = i + w + 1;
          mValue = 0;
          cLength = 0;
          continue;
        } else {
          mValue = data.get(i);
        }
      }
      if (cLength > mLength) {
        count++;
        offsetarray[count] = i + 1;
        mPos = i + w + 1;
        mValue = 0;
        cLength = 0;
      }
    } /*
       * for (int i = 0; i < size; i++) { if (data.get(i) <= mValue) { if (i ==
       * mPos + w) { count++; offsetarray[count] = i + 1; mValue = 0; } }else{
       * mValue=data.get(i); mPos = i; } }
       */
    List<Integer> offsetList = new ArrayList<Integer>();
    for (int i = 0; i < count; i++) {
      offsetList.add(offsetarray[i]);
    }
    offsetList.add(size);

    return offsetList;
  }

  /*
   * Fingerprinting class multithreaded 09/12 done
   */
  public List<byte[]> chunkHash(JedisPool jPool, List<chunkMeta> chunkMetadata,
      ByteBuffer block1, List<Integer> offset) {
    // if the number of chunk is too low, hash it with 1t.
    int nThread = this.nThread;
    if (offset.size() < 25) {
      nThread = 1;
    }
    List<byte[]> hashes = new ArrayList<byte[]>();
    // Latches for thread checking
    List<CountDownLatch> latches = new ArrayList<CountDownLatch>();
    latches.add(new CountDownLatch(1));
    new threadedHasher(jPool, chunkMetadata, offset, hashes, block1, 0, nThread,
        latches).start();
    try {
      latches.get(0).await();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return hashes;
  }

  /*
   * Search for duplicates within the file and return a list of chunks to
   * process Also update the database;
   */
  public void checkChunk(List<byte[]> hashes, List<Integer> offset,
      List<chunkMeta> chunkMetaData) {
    HashMap<byte[], chunkMeta> hMap = new HashMap<byte[], chunkMeta>();
    List<chunkMeta> reducedMD = new ArrayList<chunkMeta>();
    int t = 0;
    int fp = 0;
    int nc = 0;
    for (int i = 0; i < chunkMetaData.size(); i++) {
      if (hMap.containsKey(chunkMetaData.get(i).chunkHash)) {
        // Ok its there
        chunkMetaData.get(i).nCopy++;
        hMap.get(chunkMetaData.get(i).chunkHash).nCopy++;
        t++;
      } else {
        // False positive
        hMap.put(chunkMetaData.get(i).chunkHash, chunkMetaData.get(i));
        if (chunkMetaData.get(i).newChunk)
          storeSize += chunkMetaData.get(i).length;
        fp++;
      }
    }
    Iterator it = hMap.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry pair = (Map.Entry) it.next();
      reducedMD.add((chunkMeta) pair.getValue());
      it.remove();
    }
    chunkMetaData = null;
    chunkMetaData = reducedMD;
  }

  /*
   * Store the data in the DB 09/12 done
   */
  public void storeDB(ByteBuffer block1, List<byte[]> hashes, JedisPool jPool,
      long filename) {
    /*
     * Table #1 Block (4 bytes) | FileSize (4 bytes) | ChunksHash (num of
     * chunks*20 bytes)
     */
    // Construct the ChunksHash
    Jedis jedis = jPool.getResource();
    // Write filesize at head
    int pos = 4;
    byte[] ChunksHash = new byte[hash_length * hashes.size() + pos];
    byte[] bsize = ut1.longToBytes(size, 4);
    System.arraycopy(bsize, 0, ChunksHash, 0, bsize.length);
    for (byte[] hash : hashes) {
      System.arraycopy(hash, 0, ChunksHash, pos, hash_length);
      pos += hash_length;
    }
    jedis.set("blockID".getBytes(), ut1.blockIDtoBytes(lastBlockID));
    jedis.set(ut1.longToBytes(filename, 4), ChunksHash);
    jedis.close();
  }

  /*
   * Store the chunks as a file in a folder Table #2 ChunkHash|duplicate|cID
   * SHA1 20 bytes | duplicate 3 bytes | cid 5 bytes 2^40 max chunks
   */

  public void storeChunks(ByteBuffer block1, List<chunkMeta> toStore,
      JedisPool jPool) {
    int lastwrite = (int) lastBlockID[0 + 4];
    int maxSize = (int) Math.pow(2, 24);
    ByteBuffer bufferBB = ByteBuffer.allocate(maxSize);
    bufferBB.position(0);
    Jedis jedis = jPool.getResource();
    Pipeline pJedis = jedis.pipelined();
    String baseDir;

    if (compress)
      baseDir =
          System.getProperty("user.home") + "/testdata/chunks/compressed/";
    else
      baseDir =
          System.getProperty("user.home") + "/testdata/chunks/uncompressed/";
    byte[] buffer;
    boolean append = true;
    if (storeSize == 0) {
      // Just update the database for duplicates
      for (chunkMeta chunk : toStore) {
        pJedis.set(chunk.chunkHash, chunk.getMeta());
      }

      pJedis.sync();
      jedis.close();
      return;
    }

    try {
      // Some chunks need update
      File toWrite = new File(baseDir + lastBlockID[0]);
      int curPos = (int) toWrite.length();
      // Check new file or not
      if (!toWrite.createNewFile()) {
        // Not new
        if (toWrite.length() + storeSize > maxSize) {
          FileInputStream fs1 = new FileInputStream(toWrite);
          FileChannel fc1 = fs1.getChannel().position(0);
          fc1.read(bufferBB);
          append = false;
          fc1.close();
          fs1.close();
        }
      } else {
        // New
        curPos = 0;
      }
      for (chunkMeta chunk : toStore) {
        pJedis.set(chunk.chunkHash, chunk.getMeta());
        if (chunk.newChunk) {
          // If we need to write the chunk (non duplicate chunk)
          buffer = new byte[chunk.length];
          block1.position((int) chunk.bbStart);
          block1.get(buffer, 0, chunk.length);
          try {
            // Try to add it to the buffer
            bufferBB.put(buffer);
          } catch (Exception e) {
            // Buffer full
            // Get byte without the flag
            byte[] temp = new byte[bufferBB.position()];
            bufferBB.position(0);
            bufferBB.get(temp, 0, temp.length);
            if (compress) {
              byte[] compressed = Snappy.compress(temp);
              temp = compressed;
            }
            // add flag
            OutputStream output;
            output = new FileOutputStream(toWrite, append);
            output.write(temp);
            output.close();

            // Start with a new file for the next buffer
            lastBlockID[0]++;
            toWrite = new File(baseDir + lastBlockID[0]);
            toWrite.createNewFile();
            bufferBB.position(0);
            // Not compressed flag
            bufferBB.put(buffer);
            pJedis.sync();
            pJedis = null;
            pJedis = jedis.pipelined();
          }
          chunk.blockID = lastBlockID[0];
          chunk.blockStart = curPos;
          curPos += chunk.length;
          chunk.blockStop = curPos;
        }
      }
      byte[] temp = new byte[bufferBB.position()];
      // Last offset
      lastBlockID[0 + 4] = bufferBB.position();
      bufferBB.position(0);
      bufferBB.get(temp, 0, temp.length);
      OutputStream output;
      output = new FileOutputStream(toWrite, true);
      output.write(temp);
      // flag byte
      output.close();
      pJedis.sync();
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    jedis.close();
  }

  public void storeChunksMT(ByteBuffer block1, List<chunkMeta> toStore,
      JedisPool jPool) {
    int nThread = 1;
    if (toStore.size() < 25) {
      nThread = 1;
    } else {
      nThread = this.nThread;
    }
    // Latches for thread checking
    List<CountDownLatch> latches = new ArrayList<CountDownLatch>();
    latches.add(new CountDownLatch(1));
    new threadedStorer(jPool, storeSize, toStore, lastBlockID, block1, 0,
        nThread, latches, filename).start();
    for (CountDownLatch latch : latches) {
      try {
        latch.await();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

}

class threadedHasher implements Runnable {
  private Thread t;
  public List<byte[]> hashes;
  public List<chunkMeta> lChunkData;
  public List<chunkMeta> chunkMetadata;
  public List<byte[]> localhashes;
  public List<Integer> offset;
  public boolean done = false;
  public ByteBuffer block1;
  private int nThis;
  private int nThread;
  private int start;
  private int stop;
  private int hasher;
  Jedis jedis;
  Transaction tJedis;
  Pipeline pJedis;
  public List<CountDownLatch> latches;

  threadedHasher(JedisPool jPool, List<chunkMeta> chunkMetadata,
      List<Integer> offsets, List<byte[]> hashes, ByteBuffer block1, int nThis,
      int nThread, List<CountDownLatch> latches) {
    this.hashes = hashes;
    this.offset = offsets;
    this.chunkMetadata = chunkMetadata;
    this.block1 = block1;
    this.start = offset.size() * nThis / nThread;
    this.stop = offset.size() * (nThis + 1) / nThread;
    this.nThis = nThis;
    this.nThread = nThread;
    this.latches = latches;
    this.jedis = jPool.getResource();
    this.hasher=DataNode.hasher;
    latches.add(new CountDownLatch(1));
    if (nThis != (nThread - 1)) {
      // Execute new thread, recursive?
      new threadedHasher(jPool, chunkMetadata, offsets, hashes,
          block1.duplicate(), nThis + 1, nThread, latches).start();
    }
  }

  @Override
  public void run() {
    // TODO Auto-generated method stub
    lChunkData = new ArrayList<chunkMeta>();
    localhashes = new ArrayList<byte[]>();
    byte[] buffer;
    int curOffset = 0;
    utilities ut1 = new utilities();
    if (start != 0)
      curOffset = offset.get(start - 1);

    tJedis = jedis.multi();
    int count = 0;
    List<Response<Boolean>> lBuffer = new ArrayList<Response<Boolean>>();
    for (int ioffset = start; ioffset < stop; ioffset++) {
      /*
       * Read at an offset (from bytebuffer) and hash it
       */
      int read = offset.get(ioffset) - curOffset;
      block1.position(curOffset);
      buffer = new byte[read];
      block1.get(buffer, 0, read);
      byte[] hash=null;
      if(hasher==0)
        hash = ut1.sha1hash(buffer);
      else if (hasher == 1)
        hash = ut1.sha224hash(buffer);
      localhashes.add(hash);
      lChunkData.add(new chunkMeta(tJedis.get(hash), hash, curOffset,
          offset.get(ioffset)));
      curOffset = offset.get(ioffset);
      count++;
    }
    tJedis.exec();
    for (chunkMeta chunk : lChunkData) {
      chunk.process(-1);
    }
    // Concurrency, wait for prev.
    if (nThis != 0) {
      try {
        latches.get(nThis).await();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    /*
     * for (Response<Boolean> answer : lBuffer) { if (answer.get()) { } ; }
     */
    hashes.addAll(localhashes);
    chunkMetadata.addAll(lChunkData);
    latches.get(nThis + 1).countDown();

    // Wait the last thread, thread 0 specific
    if (nThis == 0) {
      try {
        latches.get(nThread).await();
        latches.get(0).countDown();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    jedis.close();
  }

  public void start() {
    if (t == null) {
      t = new Thread(this);
      t.start();
    }
  }

}

class threadedStorer implements Runnable {
  private Thread t;
  public List<byte[]> hashes;
  public List<chunkMeta> toStore;
  public boolean done = false;
  public ByteBuffer block1;
  int storeSize;
  long[] lastBlockID;
  private int nThis;
  private int nThread;
  private int start;
  private int stop;
  private long filename;
  Jedis jedis;
  int compressor;
  JedisPool jPool;
  Transaction tJedis;
  Pipeline pJedis;
  utilities ut1 = new utilities();
  public List<CountDownLatch> latches;
  static final Logger LOG = DataNode.LOG;
  boolean append = true;
  boolean runlog=DataNode.runlog;

  threadedStorer(JedisPool jPool, int storeSize, List<chunkMeta> toStore,
      long[] lastBlockID, ByteBuffer block1, int nThis, int nThread,
      List<CountDownLatch> latches, long filename) {

    this.filename = filename;
    this.block1 = block1;
    this.start = toStore.size() * nThis / nThread;
    this.stop = toStore.size() * (nThis + 1) / nThread;
    this.lastBlockID = lastBlockID;
    this.toStore = toStore;
    this.storeSize = storeSize;
    this.nThis = nThis;
    this.nThread = nThread;
    this.latches = latches;
    this.jPool = jPool;
    this.jedis = jPool.getResource();
    latches.add(new CountDownLatch(1));
    if (nThis != (nThread - 1)) {
      // Execute new thread, recursive?
      new threadedStorer(jPool, storeSize, toStore, lastBlockID,
          block1.duplicate(), nThis + 1, nThread, latches, filename).start();
    }
    this.compressor=DataNode.compressor;
  }

  @Override
  public void run() {
    int lastwrite = (int) lastBlockID[nThis + 4];
    int maxSize = DataDeduplicator.maxSize;
    // int maxSize = (int) Math.pow(2, 25); //32 MB
    ByteBuffer bufferBB = ByteBuffer.allocate(maxSize);
    ByteBuffer prevData = null;
    bufferBB.position(0);
    Pipeline pJedis = jedis.pipelined();
    String baseDir;
    baseDir = DataNode.chunkDir;
    byte[] buffer;
    if (storeSize == 0) {
      // Just update the database for duplicates
      for (int i = start; i < stop; i++) {
        chunkMeta chunk = toStore.get(i);
        pJedis.set(chunk.chunkHash, chunk.getMeta());
      }
      pJedis.sync();
    } else {
      // Adding chunks
      try {
        File toWrite = new File(baseDir + lastBlockID[nThis]);
        int curPos = (int) toWrite.length();
        // Manage last block
        if (!toWrite.createNewFile()) {
          // Not new
          FileInputStream fs1 = new FileInputStream(toWrite);
          FileChannel fc1 = fs1.getChannel().position(0);
          prevData = ByteBuffer.allocateDirect((int) toWrite.length());
          curPos = fc1.read(prevData);
          fc1.close();
          fs1.close();
        } else {
          // New
          curPos = 0;
        }
        // Process the chunks
        for (int i = start; i < stop; i++) {
          chunkMeta chunk = toStore.get(i);
          if (chunk.newChunk) {
            // If we need to write the chunk (non duplicate chunk)
            buffer = new byte[chunk.length];
            block1.position((int) chunk.bbStart);
            block1.get(buffer, 0, chunk.length);

            // Try to add it to the buffer
            if ((curPos + chunk.length) > maxSize) {
              // Buffer full
              // Get byte without the flag

              int bufferBBpos = bufferBB.position();
              byte[] temp;
              if (prevData != null) {
                // Add prev data first
                temp = new byte[bufferBBpos + prevData.capacity()];
                prevData.position(0);
                prevData.get(temp, 0, prevData.capacity());
                bufferBB.position(0);
                bufferBB.get(temp, prevData.capacity(), bufferBBpos);
                prevData = null;
              } else {
                temp = new byte[bufferBBpos];
                bufferBB.position(0);
                bufferBB.get(temp, 0, bufferBBpos);
              }
              FileOutputStream output;
              output = new FileOutputStream(toWrite, false);

              if (compressor==2) {
                //Dedup then compress
                //output.write(Snappy.compress(temp));
                //SnappyCodec codec = new SnappyCodec(); // Specify compression scheme?
                Lz4Codec codec = new Lz4Codec(); // Specify compression scheme?
                Configuration conf = new Configuration();
                codec.setConf(conf);
                OutputStream snappyos= codec.createOutputStream(output);
                snappyos.write(temp);
                snappyos.close();
                conf=null;
                codec=null;
              } else {
                //Dedup only
                output.write(temp);
              }
              output.close();
              output = null;
              temp = null;
              // Start with a new file for the next buffer
              bufferBB.position(0);
              curPos = 0;
              lastBlockID[nThis]++;
              lastBlockID[nThis + 4] = 0;
              toWrite = new File(baseDir + lastBlockID[nThis]);
              toWrite.createNewFile();
              // Not compressed flag
            }
            bufferBB.put(buffer);
            chunk.blockID = lastBlockID[nThis];
            chunk.setBlockStartStop(curPos);
            curPos += chunk.length;
          }
          pJedis.set(chunk.chunkHash, chunk.getMeta());

        }
        byte[] temp = new byte[bufferBB.position()];
        // Last offset
        lastBlockID[nThis + 4] = bufferBB.position();
        bufferBB.position(0);
        bufferBB.get(temp, 0, temp.length);
        bufferBB = null;
        OutputStream output;
        output = new FileOutputStream(toWrite, true);
        output.write(temp);
        // flag byte
        output.close();
        output = null;
        pJedis.sync();
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        LOG.info(e.getMessage() + "");
      }
    }
    jedis.close();
    jedis = null;

    latches.get(nThis + 1).countDown();
    if (nThis == 0) {
      latches.get(0).countDown();
    }
    block1 = null;
    if (runlog) {
      LOG.info("Thread " + nThis + " block " + filename + " done.");
    }
  }

  public void start() {
    if (t == null) {
      t = new Thread(this);
      t.start();
    }
  }

}
/*
 * class threadedStorer2 implements Runnable { private Thread t; public
 * List<byte[]> hashes; public List<chunkMeta> toStore; public boolean done =
 * false; public ByteBuffer block1; int storeSize; long[] lastBlockID; private
 * int nThis; private int nThread; private int start; private int stop; private
 * long filename; Jedis jedis; JedisPool jPool; Transaction tJedis; Pipeline
 * pJedis; utilities ut1 = new utilities(); public List<CountDownLatch> latches;
 * static final Logger LOG = DataNode.LOG;
 * 
 * threadedStorer2(JedisPool jPool, int storeSize, List<chunkMeta> toStore,
 * long[] lastBlockID, ByteBuffer block1, int nThis, int nThread,
 * List<CountDownLatch> latches, long filename) {
 * 
 * this.filename = filename; this.block1 = block1; this.start = toStore.size() *
 * nThis / nThread; this.stop = toStore.size() * (nThis + 1) / nThread;
 * this.lastBlockID = lastBlockID; this.toStore = toStore; this.storeSize =
 * storeSize; this.nThis = nThis; this.nThread = nThread; this.latches =
 * latches; this.jPool = jPool; this.jedis = jPool.getResource();
 * latches.add(new CountDownLatch(1)); if (nThis != (nThread - 1)) { // Execute
 * new thread, recursive? new threadedStorer(jPool, storeSize, toStore,
 * lastBlockID, block1.duplicate(), nThis + 1, nThread, latches,
 * filename).start(); } }
 * 
 * @Override public void run() { int lastwrite = (int) lastBlockID[nThis + 4];
 * int maxSize = DataDeduplicator.maxSize; //int maxSize = (int) Math.pow(2,
 * 25); //32 MB ByteBuffer bufferBB = ByteBuffer.allocate(maxSize); ByteBuffer
 * prevData= null; bufferBB.position(0); Pipeline pJedis = jedis.pipelined();
 * String baseDir; if(DataNode.buildCluster){ if (DataDeduplicator.compress){
 * //baseDir = "/media/ssd/testdata/chunks/compressed/"; baseDir =
 * "/media/120GBSSD/testdata/chunks/compressed/"; } else{ //baseDir =
 * "/media/ssd/testdata/chunks/uncompressed/"; baseDir =
 * "/media/120GBSSD/testdata/chunks/uncompressed/"; } }else{ if
 * (DataDeduplicator.compress) baseDir =
 * System.getProperty("user.home")+"/testdata/chunks/compressed/"; else baseDir
 * = System.getProperty("user.home")+"/testdata/chunks/uncompressed/"; } byte[]
 * buffer; boolean append = true; if (storeSize == 0) { // Just update the
 * database for duplicates for (int i = start; i < stop; i++) { chunkMeta chunk
 * = toStore.get(i); pJedis.set(chunk.chunkHash, chunk.getMeta()); }
 * pJedis.sync(); } else { // Adding chunks try { File toWrite = new
 * File(baseDir + lastBlockID[nThis]); int curPos = (int)toWrite.length();
 * //Manage last block if (!toWrite.createNewFile()) { // Not new
 * FileInputStream fs1 = new FileInputStream(toWrite); FileChannel fc1 =
 * fs1.getChannel().position(0);
 * prevData=ByteBuffer.allocateDirect((int)toWrite.length());
 * curPos=fc1.read(prevData); fc1.close(); fs1.close(); } else { // New curPos =
 * 0; } //Process the chunks for (int i = start; i < stop; i++) { chunkMeta
 * chunk = toStore.get(i); if (chunk.newChunk) { // If we need to write the
 * chunk (non duplicate chunk) buffer = new byte[chunk.length];
 * block1.position((int) chunk.bbStart); block1.get(buffer, 0, chunk.length);
 * 
 * // Try to add it to the buffer if ((curPos+chunk.length)>maxSize) { // Buffer
 * full // Get byte without the flag
 * 
 * int bufferBBpos = bufferBB.position(); byte[] temp; if (prevData != null) {
 * // Add prev data first temp = new byte[bufferBBpos + prevData.capacity()];
 * prevData.position(0); prevData.get(temp, 0, prevData.capacity());
 * bufferBB.position(0); bufferBB.get(temp, prevData.capacity(), bufferBBpos);
 * prevData = null; } else { temp = new byte[bufferBBpos]; bufferBB.position(0);
 * bufferBB.get(temp, 0, bufferBBpos); } OutputStream output; output = new
 * FileOutputStream(toWrite, false); if (DataDeduplicator.compress) {
 * output.write(Snappy.compress(temp)); }else{ output.write(temp); }
 * 
 * output.close(); output = null; temp = null; // Start with a new file for the
 * next buffer bufferBB.position(0); curPos = 0; lastBlockID[nThis]++;
 * lastBlockID[nThis + 4] = 0; toWrite = new File(baseDir + lastBlockID[nThis]);
 * toWrite.createNewFile(); // Not compressed flag } bufferBB.put(buffer);
 * chunk.blockID = lastBlockID[nThis]; chunk.setBlockStartStop(curPos); curPos
 * += chunk.length; } pJedis.set(chunk.chunkHash, chunk.getMeta());
 * 
 * } byte[] temp = new byte[bufferBB.position()]; // Last offset
 * lastBlockID[nThis + 4] = bufferBB.position(); bufferBB.position(0);
 * bufferBB.get(temp, 0, temp.length); bufferBB=null; OutputStream output;
 * output = new FileOutputStream(toWrite, true); output.write(temp); // flag
 * byte output.close(); output=null; pJedis.sync(); } catch (Exception e) { //
 * TODO Auto-generated catch block e.printStackTrace();
 * LOG.info(e.getMessage()+""); } } jedis.close(); jedis=null;
 * 
 * latches.get(nThis + 1).countDown(); if (nThis == 0) {
 * latches.get(0).countDown(); } block1=null;
 * LOG.info("Thread "+nThis+" block "+filename+" done."); }
 * 
 * public void start() { if (t == null) { t = new Thread(this); t.start(); } }
 * 
 * }
 */
