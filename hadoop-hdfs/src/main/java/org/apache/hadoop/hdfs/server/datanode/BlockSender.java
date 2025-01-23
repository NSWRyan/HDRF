/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;

import javax.swing.text.Utilities;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ReadaheadPool.ReadaheadRequest;
import org.apache.hadoop.net.SocketOutputStream;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.util.DataChecksum;
import org.apache.htrace.core.TraceScope;

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_DONTNEED;
import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_SEQUENTIAL;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import org.slf4j.Logger;

/**
 * Reads a block from the disk and sends it to a recipient.
 * 
 * Data sent from the BlockeSender in the following format: <br>
 * <b>Data format:</b>
 * 
 * <pre>
 *    +--------------------------------------------------+
 *    | ChecksumHeader | Sequence of data PACKETS...     |
 *    +--------------------------------------------------+
 * </pre>
 * 
 * <b>ChecksumHeader format:</b>
 * 
 * <pre>
 *    +--------------------------------------------------+
 *    | 1 byte CHECKSUM_TYPE | 4 byte BYTES_PER_CHECKSUM |
 *    +--------------------------------------------------+
 * </pre>
 * 
 * An empty packet is sent to mark the end of block and read completion.
 * 
 * PACKET Contains a packet header, checksum and data. Amount of data carried is
 * set by BUFFER_SIZE.
 * 
 * <pre>
 *   +-----------------------------------------------------+
 *   | Variable length header. See {@link PacketHeader}    |
 *   +-----------------------------------------------------+
 *   | x byte checksum data. x is defined below            |
 *   +-----------------------------------------------------+
 *   | actual data ......                                  |
 *   +-----------------------------------------------------+
 * 
 *   Data is made of Chunks. Each chunk is of length <= BYTES_PER_CHECKSUM.
 *   A checksum is calculated for each chunk.
 *  
 *   x = (length of data + BYTE_PER_CHECKSUM - 1)/BYTES_PER_CHECKSUM *
 *       CHECKSUM_SIZE
 *  
 *   CHECKSUM_SIZE depends on CHECKSUM_TYPE (usually, 4 for CRC32)
 * </pre>
 * 
 * The client reads data until it receives a packet with "LastPacketInBlock" set
 * to true or with a zero length. If there is no checksum error, it replies to
 * DataNode with OP_STATUS_CHECKSUM_OK.
 */
class BlockSender implements java.io.Closeable {
  static final Logger LOG = DataNode.LOG;
  static final Log ClientTraceLog = DataNode.ClientTraceLog;
  private static final boolean is32Bit =
      System.getProperty("sun.arch.data.model").equals("32");
  /**
   * Minimum buffer used while sending data to clients. Used only if
   * transferTo() is enabled. 64KB is not that large. It could be larger, but
   * not sure if there will be much more improvement.
   */
  private static final int MIN_BUFFER_WITH_TRANSFERTO = 64 * 1024;
  private static final int IO_FILE_BUFFER_SIZE;
  static {
    HdfsConfiguration conf = new HdfsConfiguration();
    IO_FILE_BUFFER_SIZE = DFSUtilClient.getIoFileBufferSize(conf);
  }
  private static final int TRANSFERTO_BUFFER_SIZE =
      Math.max(IO_FILE_BUFFER_SIZE, MIN_BUFFER_WITH_TRANSFERTO);

  /** the block to read from */
  private final ExtendedBlock block;

  /** InputStreams and file descriptors to read block/checksum. */
  private ReplicaInputStreams ris;
  /** updated while using transferTo() */
  private long blockInPosition = -1;
  /** Checksum utility */
  private final DataChecksum checksum;
  /** Initial position to read */
  private long initialOffset;
  /** Current position of read */
  private long offset;
  /** Position of last byte to read from block file */
  private final long endOffset;
  /** Number of bytes in chunk used for computing checksum */
  private final int chunkSize;
  /** Number bytes of checksum computed for a chunk */
  private final int checksumSize;
  /** If true, failure to read checksum is ignored */
  private final boolean corruptChecksumOk;
  /** Sequence number of packet being sent */
  private long seqno;
  /** Set to true if transferTo is allowed for sending data to the client */
  private boolean transferToAllowed;
  /**
   * Set to true once entire requested byte range has been sent to the client
   */
  private boolean sentEntireByteRange;
  /** When true, verify checksum while reading from checksum file */
  private final boolean verifyChecksum;
  /** Format used to print client trace log messages */
  private final String clientTraceFmt;
  private volatile ChunkChecksum lastChunkChecksum = null;
  private DataNode datanode;

  /** The replica of the block that is being read. */
  private final Replica replica;

  // Cache-management related fields
  private final long readaheadLength;

  private ReadaheadRequest curReadahead;

  private final boolean alwaysReadahead;

  private final boolean dropCacheBehindLargeReads;

  private final boolean dropCacheBehindAllReads;

  private long lastCacheDropOffset;
  private final FileIoProvider fileIoProvider;

  DataConstructor dConstructor = null;
  long dcBlockLength;
  FileInputStream fis;
  boolean runNormally = true;
  byte[] normalReadData;
  InputStream blockIn = null;
  boolean runlog = DataNode.runlog;
  long blockID = 0;
  byte[] t1data;
  @VisibleForTesting
  static long CACHE_DROP_INTERVAL_BYTES = 1024 * 1024; // 1MB

  /**
   * See {{@link BlockSender#isLongRead()}
   */
  private static final long LONG_READ_THRESHOLD_BYTES = 256 * 1024;

  // The number of bytes per checksum here determines the alignment
  // of reads: we always start reading at a checksum chunk boundary,
  // even if the checksum type is NULL. So, choosing too big of a value
  // would risk sending too much unnecessary data. 512 (1 disk sector)
  // is likely to result in minimal extra IO.
  private static final long CHUNK_SIZE = 512;

  /**
   * Constructor
   * 
   * @param block Block that is being read
   * @param startOffset starting offset to read from
   * @param length length of data to read
   * @param corruptChecksumOk if true, corrupt checksum is okay
   * @param verifyChecksum verify checksum while reading the data
   * @param sendChecksum send checksum to client.
   * @param datanode datanode from which the block is being read
   * @param clientTraceFmt format string used to print client trace logs
   * @throws IOException
   */
  BlockSender(ExtendedBlock block, long startOffset, long length,
      boolean corruptChecksumOk, boolean verifyChecksum, boolean sendChecksum,
      DataNode datanode, String clientTraceFmt, CachingStrategy cachingStrategy)
      throws IOException {
    DataInputStream checksumIn = null;
    FsVolumeReference volumeRef = null;
    this.fileIoProvider = datanode.getFileIoProvider();
    try {
      this.block = block;
      this.corruptChecksumOk = corruptChecksumOk;
      this.verifyChecksum = verifyChecksum;
      this.clientTraceFmt = clientTraceFmt;

      /*
       * Plastamod check block on Redis first
       */
      if (datanode.modrun) {
        /*
        blockID = block.getBlockId();
        if (DataNode.preBuildCoolDown == 0
            || DataNode.preBuildDataBlockID == blockID) {
          // If data is already preBuildly preloaded.
          runNormally = false;
          //Wait if preBuild run is not done yet.
          while(!DataNode.preBuildDataDone){
            try{
            Thread.sleep(10);}
            catch(Exception e){
              LOG.info("preBuild sleep error " + e );
            }
          }
          if (runlog)LOG.info("preBuild run " + blockID);
        } else {
          if(DataNode.preBuildDataDone){
            //mismatch with prebuild prediction, disable prebuild
            //DataNode.preBuildCoolDown=1;
            DataNode.preBuildCoolDown=1;
          }
          DataNode.preBuildCoolDown=1;
          // Start anew.
          try {
            utilities ut1 = new utilities();
            Jedis jedis = new Jedis("localhost");
            t1data = jedis.get(ut1.longToBytes(blockID, 4));
            if (t1data != null) {
              runNormally = false;
            }else{
              //MapRedrun
              DataNode.preBuildCoolDown=1;
            }
            if (runlog)
              LOG.info("runNormally " + runNormally + " block setNumBytes "
                  + block.getNumBytes());
            jedis.close();
            jedis = null;
          } catch (Exception e) {
            LOG.info("Error " + e + " block check error");
          }
        }
        */
        //PlastaMOD
        blockID=block.getBlockId();
        if(DataNode.compressor==-1){
          //LOG.info("No compressor rebuild "+block.getBlockId());
          File uncompressedblock =
              new File(DataNode.chunkDir + block.getBlockId());
          if (uncompressedblock.exists()) {
            fis = new FileInputStream(uncompressedblock);
            fis.getChannel().position(offset);
            blockIn = fis;
            dcBlockLength = uncompressedblock.length();
            //LOG.info("Offset "+offset+" length "+length+" fispos "+ fis.getChannel().position());
            this.transferToAllowed = false;
            runNormally = false;
          } else {
            //LOG.info("MapredRun "+block.getBlockId());
            runNormally = true;
          }
        }else{
          try {
            utilities ut1 = new utilities();
            Jedis jedis = new Jedis("localhost");
            t1data = jedis.get(ut1.longToBytes(blockID, 4));
            if (t1data != null) {
              runNormally = false;
            } else {
              // MapRedrun
              DataNode.preBuildCoolDown = 1;
            }
            if (runlog)
              LOG.info("runNormally " + runNormally + " block setNumBytes "
                  + block.getNumBytes());
            jedis.close();
            jedis = null;
          } catch (Exception e) {
            LOG.info("Error " + e + " block check error");
          }
        }
      }
      /*
       * If the client asked for the cache to be dropped behind all reads, we
       * honor that. Otherwise, we use the DataNode defaults. When using
       * DataNode defaults, we use a heuristic where we only drop the cache for
       * large reads.
       */
      if (cachingStrategy.getDropBehind() == null) {
        this.dropCacheBehindAllReads = false;
        this.dropCacheBehindLargeReads =
            datanode.getDnConf().dropCacheBehindReads;
      } else {
        this.dropCacheBehindAllReads = this.dropCacheBehindLargeReads =
            cachingStrategy.getDropBehind().booleanValue();
      }
      /*
       * Similarly, if readahead was explicitly requested, we always do it.
       * Otherwise, we read ahead based on the DataNode settings, and only when
       * the reads are large.
       */
      if (cachingStrategy.getReadahead() == null) {
        this.alwaysReadahead = false;
        this.readaheadLength = datanode.getDnConf().readaheadLength;
      } else {
        this.alwaysReadahead = true;
        this.readaheadLength = cachingStrategy.getReadahead().longValue();
      }
      this.datanode = datanode;

      if (verifyChecksum) {
        // To simplify implementation, callers may not specify verification
        // without sending.
        Preconditions.checkArgument(sendChecksum,
            "If verifying checksum, currently must also send it.");
      }

      // if there is a append write happening right after the BlockSender
      // is constructed, the last partial checksum maybe overwritten by the
      // append, the BlockSender need to use the partial checksum before
      // the append write.
      ChunkChecksum chunkChecksum = null;
      final long replicaVisibleLength;
      try (AutoCloseableLock lock = datanode.data.acquireDatasetLock()) {
        replica = getReplica(block, datanode);
        //PlastaMOD lengthcheck bypass
        if (runlog)
          LOG.info("replica " + replica.getBytesOnDisk());
        if (!runNormally) {
          replica.setNumBytes(block.getNumBytes());
          replicaVisibleLength = block.getNumBytes();
        } else {
          replicaVisibleLength = replica.getVisibleLength();
        }
      }
      if (replica.getState() == ReplicaState.RBW) {
        final ReplicaInPipeline rbw = (ReplicaInPipeline) replica;
        waitForMinLength(rbw, startOffset + length);
        chunkChecksum = rbw.getLastChecksumAndDataLen();
      }
      if (replica instanceof FinalizedReplica) {
        chunkChecksum =
            getPartialChunkChecksumForFinalized((FinalizedReplica) replica);
      }

      if (replica.getGenerationStamp() < block.getGenerationStamp()) {
        throw new IOException("Replica gen stamp < block genstamp, block="
            + block + ", replica=" + replica);
      } else if (replica.getGenerationStamp() > block.getGenerationStamp()) {
        if (DataNode.LOG.isDebugEnabled()) {
          DataNode.LOG.debug(
              "Bumping up the client provided" + " block's genstamp to latest "
                  + replica.getGenerationStamp() + " for block " + block);
        }
        block.setGenerationStamp(replica.getGenerationStamp());
      }
      if (replicaVisibleLength < 0) {
        throw new IOException(
            "Replica is not readable, block=" + block + ", replica=" + replica);
      }
      if (DataNode.LOG.isDebugEnabled()) {
        DataNode.LOG.debug("block=" + block + ", replica=" + replica);
      }
      // Plasta transfertomod
      // transferToFully() fails on 32 bit platforms for block sizes >= 2GB,
      // use normal transfer in those cases
      this.transferToAllowed = datanode.getDnConf().transferToAllowed &&
      (!is32Bit || length <= Integer.MAX_VALUE);

      // Obtain a reference before reading data
      volumeRef = datanode.data.getVolume(block).obtainReference();

      /*
       * (corruptChecksumOK, meta_file_exist): operation True, True: will verify
       * checksum True, False: No verify, e.g., need to read data from a
       * corrupted file False, True: will verify checksum False, False: throws
       * IOException file not found
       */
      DataChecksum csum = null;
      if (verifyChecksum || sendChecksum) {
        LengthInputStream metaIn = null;
        boolean keepMetaInOpen = false;
        try {
          DataNodeFaultInjector.get().throwTooManyOpenFiles();
          metaIn = datanode.data.getMetaDataInputStream(block);
          if (!corruptChecksumOk || metaIn != null) {
            if (metaIn == null) {
              // need checksum but meta-data not found
              throw new FileNotFoundException(
                  "Meta-data not found for " + block);
            }

            // The meta file will contain only the header if the NULL checksum
            // type was used, or if the replica was written to transient
            // storage.
            // Also, when only header portion of a data packet was transferred
            // and then pipeline breaks, the meta file can contain only the
            // header and 0 byte in the block data file.
            // Checksum verification is not performed for replicas on transient
            // storage. The header is important for determining the checksum
            // type later when lazy persistence copies the block to
            // non-transient
            // storage and computes the checksum.
            if (!replica.isOnTransientStorage()
                && metaIn.getLength() >= BlockMetadataHeader.getHeaderSize()) {
              checksumIn = new DataInputStream(
                  new BufferedInputStream(metaIn, IO_FILE_BUFFER_SIZE));

              csum = BlockMetadataHeader.readDataChecksum(checksumIn, block);
              keepMetaInOpen = true;
            }
          } else {
            LOG.warn("Could not find metadata file for " + block);
          }
        } catch (FileNotFoundException e) {
          if ((e.getMessage() != null)
              && !(e.getMessage().contains("Too many open files"))) {
            // The replica is on its volume map but not on disk
            datanode.notifyNamenodeDeletedBlock(block,
                replica.getStorageUuid());
            datanode.data.invalidate(block.getBlockPoolId(),
                new Block[] { block.getLocalBlock() });
          }
          throw e;
        } finally {
          if (!keepMetaInOpen) {
            IOUtils.closeStream(metaIn);
          }
        }
      }
      if (csum == null) {
        csum = DataChecksum.newDataChecksum(DataChecksum.Type.NULL,
            (int) CHUNK_SIZE);
      }

      /*
       * If chunkSize is very large, then the metadata file is mostly corrupted.
       * For now just truncate bytesPerchecksum to blockLength.
       */
      int size = csum.getBytesPerChecksum();
      if (size > 10 * 1024 * 1024 && size > replicaVisibleLength) {
        csum = DataChecksum.newDataChecksum(csum.getChecksumType(),
            Math.max((int) replicaVisibleLength, 10 * 1024 * 1024));
        size = csum.getBytesPerChecksum();
      }
      chunkSize = size;
      checksum = csum;
      checksumSize = checksum.getChecksumSize();

      if (runlog)
        LOG.info("block " + block.getBlockId() + " length = " + length
            + " replicaVisibleLength = " + replicaVisibleLength
            + "replica.getBytesOnDisk()" + replica.getBytesOnDisk());
      length = length < 0 ? replicaVisibleLength : length;

      // end is either last byte on disk or the length for which we have a
      // checksum
      long end = chunkChecksum != null ? chunkChecksum.getDataLength()
          : replica.getBytesOnDisk();

      if (startOffset < 0 || startOffset > end
          || (length + startOffset) > end) {
        String msg = " Offset " + startOffset + " and length " + length
            + " don't match block " + block + " ( blockLen " + end + " )";
        // LOG.warn(datanode.getDNRegistrationForBP(block.getBlockPoolId())
        // + ":sendBlock() : " + msg);
        // throw new IOException(msg);
      }

      // Ensure read offset is position at the beginning of chunk
      offset = startOffset - (startOffset % chunkSize);
      if (length >= 0) {
        // Ensure endOffset points to end of chunk.
        long tmpLen = startOffset + length;
        if (tmpLen % chunkSize != 0) {
          tmpLen += (chunkSize - tmpLen % chunkSize);
        }
        if (tmpLen < end) {
          // will use on-disk checksum here since the end is a stable chunk
          end = tmpLen;
        } else if (chunkChecksum != null) {
          // last chunk is changing. flag that we need to use in-memory checksum
          this.lastChunkChecksum = chunkChecksum;
        }
      }
      endOffset = end;

      // seek to the right offsets
      if (offset > 0 && checksumIn != null) {
        long checksumSkip = (offset / chunkSize) * checksumSize;
        // note blockInStream is seeked when created below
        if (checksumSkip > 0) {
          // Should we use seek() for checksum file as well?
          IOUtils.skipFully(checksumIn, checksumSkip);
        }
      }
      seqno = 0;

      
        /*
         * File asd = new File("/home/ryan/testdata/blk_" +
         * replica.getBlockId()); LOG.info("replica = " + replica.getBlockId());
         * FileInputStream fs1 = new FileInputStream(asd); FileChannel fc1 =
         * fs1.getChannel().position(offset); ByteBuffer bf1 =
         * ByteBuffer.wrap(new byte[(int) (asd.length() - offset)]); int read =
         * fc1.read(bf1); bf1.flip(); fc1.close(); LOG.info( "chunkChecksum  " +
         * (chunkChecksum != null) + " getDataLength = " +
         * chunkChecksum.getDataLength() + " replica.getBytesOnDisk() = " +
         * replica.getBytesOnDisk() + " read = " + read); blockIn = new
         * ByteArrayInputStream(bf1.array()); end = read;
         */
        //Setup for preBuild
        /*
        try {
          long before = System.currentTimeMillis();
          if (DataNode.preBuildDataBlockID == blockID) {
            if (runlog)LOG.info("preBuild run " + blockID);
            blockIn = new ByteArrayInputStream(DataNode.preBuildData,
                (int) offset, DataNode.preBuildData.length);
            end = DataNode.preBuildData.length;
          } else {
            if (runlog)LOG.info("Normal build " + blockID);
            DataNode.rebuildctr++;
            LOG.info("Rebuilding block " + blockID + "job ctr " +DataNode.rebuildctr);
            dConstructor = new DataConstructor(replica.getBlockId(), t1data);
            blockIn = new ByteArrayInputStream(dConstructor.data, (int) offset,
                dConstructor.data.length);
            if (runlog)
              LOG.info("Rebuilding {} size {} of data took {} ms",
                  replica.getBlockId(), dConstructor.data.length,
                  System.currentTimeMillis() - before);
            if (runlog)
              LOG.info("Read offset " + offset + " startOffset " + startOffset
                  + " length " + length);
            if (runlog)
              LOG.info("dConstructor.data " + dConstructor.data.length);
            end = dConstructor.filesize;
          }
          this.transferToAllowed = false;
        } catch (Exception e) {
          LOG.info("DC crash" + e);
          this.transferToAllowed = true;
        }*/
        /*
         * Debug LOG.info("Read offset "+offset+ " startOffset " + startOffset
         * + " length " + length);
         * 
         * LOG.info("dConstructor.data "+dConstructor.data.length);
         * 
         * utilities ut1 = new utilities(); LOG.info("Block "+
         * replica.getBlockId()+ " HASH "+
         * ut1.byteToHex(ut1.sha1hash(dConstructor.data)));
         * 
         * OutputStream output3; output3 = new FileOutputStream( new
         * File("/home/ryan/testdata/Swim/blockOut/"+ replica.getBlockId()),
         * false); output3.write(dConstructor.data); output3.close();
         */
        //Plasta run preBuild build
        //if(System.currentTimeMillis()>DataNode.preBuildCoolDown){
        /*if(DataNode.preBuildCoolDown==0){
          new preBuildDC(blockID).start();
        }*/
        //
        //PlastaMOD
      if (DataNode.compressor != -1 && !runNormally) {
        if (runlog)
          LOG.info("DC build " + blockID);
        dConstructor = new DataConstructor(replica.getBlockId(), t1data);
        blockIn = new ByteArrayInputStream(dConstructor.data, (int) offset,
            dConstructor.data.length);
        end = dConstructor.filesize;
        this.transferToAllowed = false;
      } else {
        // NormalRebuild
        end = dcBlockLength;
      }

      try {
        int currentQ = DataXceiver.AIReadTurn.get();
        if (runlog)LOG.info("bs #" + DataXceiver.AIReadTurn.get()
        + " incrementing queue.");
        while (currentQ >= DataXceiver.AIReadTurn.incrementAndGet()) {
          if (runlog)
            LOG.info("bs incrementing queue.");
          Thread.sleep(10);
        }
      } catch (Exception e) {
        LOG.info("BlockSender DataXceiver queue can't resolve queue" + e);
      }

      // @Plastamod
      if (!DataNode.modrun || runNormally) {
        if (runlog)
          LOG.info("runNormally setting blockIn block : " + block.getBlockId()
              + " offset : " + offset + " endOffset " + endOffset);
        blockIn = datanode.data.getBlockInputStream(block, offset); // seek to offset
        //LOG.info("MR block run normal"+block.getBlockId());
        // modend
      }
      if (runlog)
        LOG.info("Transferto : " + transferToAllowed);
      ris = new ReplicaInputStreams(blockIn, checksumIn, volumeRef,
          fileIoProvider);
    } catch (IOException ioe) {
      IOUtils.closeStream(this);
      org.apache.commons.io.IOUtils.closeQuietly(blockIn);
      org.apache.commons.io.IOUtils.closeQuietly(fis);
      org.apache.commons.io.IOUtils.closeQuietly(checksumIn);
      throw ioe;
    }
  }

  private ChunkChecksum getPartialChunkChecksumForFinalized(
      FinalizedReplica finalized) throws IOException {
    // There are a number of places in the code base where a finalized replica
    // object is created. If last partial checksum is loaded whenever a
    // finalized replica is created, it would increase latency in DataNode
    // initialization. Therefore, the last partial chunk checksum is loaded
    // lazily.

    // Load last checksum in case the replica is being written concurrently
    final long replicaVisibleLength = replica.getVisibleLength();
    if (replicaVisibleLength % CHUNK_SIZE != 0
        && finalized.getLastPartialChunkChecksum() == null) {
      // the finalized replica does not have precomputed last partial
      // chunk checksum. Recompute now.
      try {
        finalized.loadLastPartialChunkChecksum();
        return new ChunkChecksum(finalized.getVisibleLength(),
            finalized.getLastPartialChunkChecksum());
      } catch (FileNotFoundException e) {
        // meta file is lost. Continue anyway to preserve existing behavior.
        DataNode.LOG
            .warn("meta file " + finalized.getMetaFile() + " is missing!");
        return null;
      }
    } else {
      // If the checksum is null, BlockSender will use on-disk checksum.
      return new ChunkChecksum(finalized.getVisibleLength(),
          finalized.getLastPartialChunkChecksum());
    }
  }

  /**
   * close opened files.
   */
  @Override
  public void close() throws IOException {
    if (ris.getDataInFd() != null && ((dropCacheBehindAllReads)
        || (dropCacheBehindLargeReads && isLongRead()))) {
      try {
        ris.dropCacheBehindReads(block.getBlockName(), lastCacheDropOffset,
            offset - lastCacheDropOffset, POSIX_FADV_DONTNEED);
      } catch (Exception e) {
        LOG.warn("Unable to drop cache on file close", e);
      }
    }
    if (curReadahead != null) {
      curReadahead.cancel();
    }

    try {
      ris.closeStreams();
    } finally {
      IOUtils.closeStream(ris);
      ris = null;
    }
    if (!runNormally) {
      try {
        dConstructor.data = null;
      } catch (Exception e) {
      }
      dConstructor = null;
      normalReadData = null;
    }
  }

  private static Replica getReplica(ExtendedBlock block, DataNode datanode)
      throws ReplicaNotFoundException {
    Replica replica =
        datanode.data.getReplica(block.getBlockPoolId(), block.getBlockId());
    if (replica == null) {
      throw new ReplicaNotFoundException(block);
    }
    return replica;
  }

  /**
   * Wait for rbw replica to reach the length
   * 
   * @param rbw replica that is being written to
   * @param len minimum length to reach
   * @throws IOException on failing to reach the len in given wait time
   */
  private static void waitForMinLength(ReplicaInPipeline rbw, long len)
      throws IOException {
    // Wait for 3 seconds for rbw replica to reach the minimum length
    for (int i = 0; i < 30 && rbw.getBytesOnDisk() < len; i++) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
    long bytesOnDisk = rbw.getBytesOnDisk();
    if (bytesOnDisk < len) {
      throw new IOException(String.format(
          "Need %d bytes, but only %d bytes available", len, bytesOnDisk));
    }
  }

  /**
   * Converts an IOExcpetion (not subclasses) to SocketException. This is
   * typically done to indicate to upper layers that the error was a socket
   * error rather than often more serious exceptions like disk errors.
   */
  private static IOException ioeToSocketException(IOException ioe) {
    if (ioe.getClass().equals(IOException.class)) {
      // "se" could be a new class in stead of SocketException.
      IOException se = new SocketException("Original Exception : " + ioe);
      se.initCause(ioe);
      /*
       * Change the stacktrace so that original trace is not truncated when
       * printed.
       */
      se.setStackTrace(ioe.getStackTrace());
      return se;
    }
    // otherwise just return the same exception.
    return ioe;
  }

  /**
   * @param datalen Length of data
   * @return number of chunks for data of given size
   */
  private int numberOfChunks(long datalen) {
    return (int) ((datalen + chunkSize - 1) / chunkSize);
  }

  /**
   * Sends a packet with up to maxChunks chunks of data.
   * 
   * @param pkt buffer used for writing packet data
   * @param maxChunks maximum number of chunks to send
   * @param out stream to send data to
   * @param transferTo use transferTo to send data
   * @param throttler used for throttling data transfer bandwidth
   */
  private int sendPacket(ByteBuffer pkt, int maxChunks, OutputStream out,
      boolean transferTo, DataTransferThrottler throttler) throws IOException {
    /*
     * Plasta utilities ut1 = new utilities(); MessageDigest md = null; try {
     * pkt.position(0); byte[] buffercontent=new byte[(int) pkt.capacity()];
     * pkt.get(buffercontent,0,(int) pkt.capacity()); md =
     * MessageDigest.getInstance("SHA-1"); md.update(buffercontent); byte[]
     * mdbytes = md.digest(); FileWriter fileWriter = new
     * FileWriter("/home/ryan/testdata/loghashmk3", true); PrintWriter
     * printWriter = new PrintWriter(fileWriter); printWriter.println(new
     * SimpleDateFormat("yyyy/MM/dd_HH:mm:ss")
     * .format(Calendar.getInstance().getTime()) +"\t" +ut1.byteToHex(mdbytes));
     * printWriter.close(); fileWriter.close();
     * 
     * }catch(Exception e){
     * 
     * } pkt.position(curpos);
     */
    int dataLen =
        (int) Math.min(endOffset - offset, (chunkSize * (long) maxChunks));

    int numChunks = numberOfChunks(dataLen); // Number of chunks be sent in the
                                             // packet
    int checksumDataLen = numChunks * checksumSize;
    int packetLen = dataLen + checksumDataLen + 4;
    boolean lastDataPacket = offset + dataLen == endOffset && dataLen > 0;

    // The packet buffer is organized as follows:
    // _______HHHHCCCCD?D?D?D?
    // ^ ^
    // | \ checksumOff
    // \ headerOff
    // _ padding, since the header is variable-length
    // H = header and length prefixes
    // C = checksums
    // D? = data, if transferTo is false.

    int headerLen = writePacketHeader(pkt, dataLen, packetLen);

    // Per above, the header doesn't start at the beginning of the
    // buffer
    int headerOff = pkt.position() - headerLen;

    int checksumOff = pkt.position();
    byte[] buf = pkt.array();

    if (checksumSize > 0 && ris.getChecksumIn() != null) {
      readChecksum(buf, checksumOff, checksumDataLen);

      // write in progress that we need to use to get last checksum
      if (lastDataPacket && lastChunkChecksum != null) {
        int start = checksumOff + checksumDataLen - checksumSize;
        byte[] updatedChecksum = lastChunkChecksum.getChecksum();
        if (updatedChecksum != null) {
          System.arraycopy(updatedChecksum, 0, buf, start, checksumSize);
        }
      }
    }

    int dataOff = checksumOff + checksumDataLen;
    if (!transferTo) { // normal transfer
      ris.readDataFully(buf, dataOff, dataLen);

      if (verifyChecksum) {
        verifyChecksum(buf, dataOff, dataLen, numChunks, checksumOff);
      }
    }

    try {
      if (transferTo) {
        SocketOutputStream sockOut = (SocketOutputStream) out;
        // First write header and checksums
        sockOut.write(buf, headerOff, dataOff - headerOff);

        // no need to flush since we know out is not a buffered stream
        FileChannel fileCh = ((FileInputStream) ris.getDataIn()).getChannel();
        LongWritable waitTime = new LongWritable();
        LongWritable transferTime = new LongWritable();
        fileIoProvider.transferToSocketFully(ris.getVolumeRef().getVolume(),
            sockOut, fileCh, blockInPosition, dataLen, waitTime, transferTime);
        datanode.metrics.addSendDataPacketBlockedOnNetworkNanos(waitTime.get());
        datanode.metrics.addSendDataPacketTransferNanos(transferTime.get());
        blockInPosition += dataLen;
      } else {
        // normal transfer
        out.write(buf, headerOff, dataOff + dataLen - headerOff);
      }
    } catch (IOException e) {
      if (e instanceof SocketTimeoutException) {
        /*
         * writing to client timed out. This happens if the client reads part of
         * a block and then decides not to read the rest (but leaves the socket
         * open).
         * 
         * Reporting of this case is done in DataXceiver#run
         */
      } else {
        /*
         * Exception while writing to the client. Connection closure from the
         * other end is mostly the case and we do not care much about it. But
         * other things can go wrong, especially in transferTo(), which we do
         * not want to ignore.
         *
         * The message parsing below should not be considered as a good coding
         * example. NEVER do it to drive a program logic. NEVER. It was done
         * here because the NIO throws an IOException for EPIPE.
         */
        String ioem = e.getMessage();
        if (!ioem.startsWith("Broken pipe")
            && !ioem.startsWith("Connection reset")) {
          LOG.error("BlockSender.sendChunks() exception: ", e);
          datanode.getBlockScanner().markSuspectBlock(
              ris.getVolumeRef().getVolume().getStorageID(), block);
        }
      }
      throw ioeToSocketException(e);
    }

    if (throttler != null) { // rebalancing so throttle
      throttler.throttle(packetLen);
    }

    return dataLen;
  }

  /**
   * Read checksum into given buffer
   * 
   * @param buf buffer to read the checksum into
   * @param checksumOffset offset at which to write the checksum into buf
   * @param checksumLen length of checksum to write
   * @throws IOException on error
   */
  private void readChecksum(byte[] buf, final int checksumOffset,
      final int checksumLen) throws IOException {
    if (checksumSize <= 0 && ris.getChecksumIn() == null) {
      return;
    }
    try {
      ris.readChecksumFully(buf, checksumOffset, checksumLen);
    } catch (IOException e) {
      LOG.warn(" Could not read or failed to verify checksum for data"
          + " at offset " + offset + " for block " + block, e);
      ris.closeChecksumStream();
      if (corruptChecksumOk) {
        if (checksumOffset < checksumLen) {
          // Just fill the array with zeros.
          Arrays.fill(buf, checksumOffset, checksumLen, (byte) 0);
        }
      } else {
        throw e;
      }
    }
  }

  /**
   * Compute checksum for chunks and verify the checksum that is read from the
   * metadata file is correct.
   * 
   * @param buf buffer that has checksum and data
   * @param dataOffset position where data is written in the buf
   * @param datalen length of data
   * @param numChunks number of chunks corresponding to data
   * @param checksumOffset offset where checksum is written in the buf
   * @throws ChecksumException on failed checksum verification
   */
  public void verifyChecksum(final byte[] buf, final int dataOffset,
      final int datalen, final int numChunks, final int checksumOffset)
      throws ChecksumException {
    int dOff = dataOffset;
    int cOff = checksumOffset;
    int dLeft = datalen;

    for (int i = 0; i < numChunks; i++) {
      checksum.reset();
      int dLen = Math.min(dLeft, chunkSize);
      checksum.update(buf, dOff, dLen);
      if (!checksum.compare(buf, cOff)) {
        long failedPos = offset + datalen - dLeft;
        StringBuilder replicaInfoString = new StringBuilder();
        if (replica != null) {
          replicaInfoString.append(" for replica: " + replica.toString());
        }
        throw new ChecksumException(
            "Checksum failed at " + failedPos + replicaInfoString, failedPos);
      }
      dLeft -= dLen;
      dOff += dLen;
      cOff += checksumSize;
    }
  }

  /**
   * sendBlock() is used to read block and its metadata and stream the data to
   * either a client or to another datanode.
   * 
   * @param out stream to which the block is written to
   * @param baseStream optional. if non-null, <code>out</code> is assumed to be
   *          a wrapper over this stream. This enables optimizations for sending
   *          the data, e.g.
   *          {@link SocketOutputStream#transferToFully(FileChannel, long, int)}.
   * @param throttler for sending data.
   * @return total bytes read, including checksum data.
   */
  long sendBlock(DataOutputStream out, OutputStream baseStream,
      DataTransferThrottler throttler) throws IOException {
    final TraceScope scope =
        datanode.getTracer().newScope("sendBlock_" + block.getBlockId());
    try {
      return doSendBlock(out, baseStream, throttler);
    } finally {
      scope.close();
    }
  }

  private long doSendBlock(DataOutputStream out, OutputStream baseStream,
      DataTransferThrottler throttler) throws IOException {
    if (out == null) {
      throw new IOException("out stream is null");
    }
    initialOffset = offset;
    long totalRead = 0;
    OutputStream streamForSendChunks = out;

    lastCacheDropOffset = initialOffset;

    if (isLongRead() && ris.getDataInFd() != null) {
      // Advise that this file descriptor will be accessed sequentially.
      ris.dropCacheBehindReads(block.getBlockName(), 0, 0,
          POSIX_FADV_SEQUENTIAL);
    }

    // Trigger readahead of beginning of file if configured.
    manageOsCache();

    final long startTime =
        ClientTraceLog.isDebugEnabled() ? System.nanoTime() : 0;
    try {
      int maxChunksPerPacket;
      int pktBufSize = PacketHeader.PKT_MAX_HEADER_LEN;
      boolean transferTo = transferToAllowed && !verifyChecksum
          && baseStream instanceof SocketOutputStream
          && ris.getDataIn() instanceof FileInputStream;
      if (transferTo) {
        FileChannel fileChannel =
            ((FileInputStream) ris.getDataIn()).getChannel();
        blockInPosition = fileChannel.position();
        streamForSendChunks = baseStream;
        maxChunksPerPacket = numberOfChunks(TRANSFERTO_BUFFER_SIZE);

        // Smaller packet size to only hold checksum when doing transferTo
        pktBufSize += checksumSize * maxChunksPerPacket;
      } else {
        maxChunksPerPacket = Math.max(1, numberOfChunks(IO_FILE_BUFFER_SIZE));
        // Packet size includes both checksum and data
        pktBufSize += (chunkSize + checksumSize) * maxChunksPerPacket;
      }

      ByteBuffer pktBuf = ByteBuffer.allocate(pktBufSize);

      while (endOffset > offset && !Thread.currentThread().isInterrupted()) {
        manageOsCache();
        long len = sendPacket(pktBuf, maxChunksPerPacket, streamForSendChunks,
            transferTo, throttler);
        offset += len;
        totalRead += len + (numberOfChunks(len) * checksumSize);
        seqno++;
      }
      // If this thread was interrupted, then it did not send the full block.
      if (!Thread.currentThread().isInterrupted()) {
        try {
          // send an empty packet to mark the end of the block
          sendPacket(pktBuf, maxChunksPerPacket, streamForSendChunks,
              transferTo, throttler);
          out.flush();
        } catch (IOException e) { // socket error
          throw ioeToSocketException(e);
        }

        sentEntireByteRange = true;
      }
    } finally {
      if ((clientTraceFmt != null) && ClientTraceLog.isDebugEnabled()) {
        final long endTime = System.nanoTime();
        ClientTraceLog.debug(String.format(clientTraceFmt, totalRead,
            initialOffset, endTime - startTime));
      }
      close();
      org.apache.commons.io.IOUtils.closeQuietly(blockIn);
      org.apache.commons.io.IOUtils.closeQuietly(fis);

    }
    return totalRead;
  }

  /**
   * Manage the OS buffer cache by performing read-ahead and drop-behind.
   */
  private void manageOsCache() throws IOException {
    // We can't manage the cache for this block if we don't have a file
    // descriptor to work with.
    if (ris.getDataInFd() == null) {
      return;
    }

    // Perform readahead if necessary
    if ((readaheadLength > 0) && (datanode.readaheadPool != null)
        && (alwaysReadahead || isLongRead())) {
      curReadahead = datanode.readaheadPool.readaheadStream(clientTraceFmt,
          ris.getDataInFd(), offset, readaheadLength, Long.MAX_VALUE,
          curReadahead);
    }

    // Drop what we've just read from cache, since we aren't
    // likely to need it again
    if (dropCacheBehindAllReads
        || (dropCacheBehindLargeReads && isLongRead())) {
      long nextCacheDropOffset =
          lastCacheDropOffset + CACHE_DROP_INTERVAL_BYTES;
      if (offset >= nextCacheDropOffset) {
        long dropLength = offset - lastCacheDropOffset;
        ris.dropCacheBehindReads(block.getBlockName(), lastCacheDropOffset,
            dropLength, POSIX_FADV_DONTNEED);
        lastCacheDropOffset = offset;
      }
    }
  }

  /**
   * Returns true if we have done a long enough read for this block to qualify
   * for the DataNode-wide cache management defaults. We avoid applying the
   * cache management defaults to smaller reads because the overhead would be
   * too high.
   *
   * Note that if the client explicitly asked for dropBehind, we will do it even
   * on short reads.
   * 
   * This is also used to determine when to invoke
   * posix_fadvise(POSIX_FADV_SEQUENTIAL).
   */
  private boolean isLongRead() {
    return (endOffset - initialOffset) > LONG_READ_THRESHOLD_BYTES;
  }

  /**
   * Write packet header into {@code pkt}, return the length of the header
   * written.
   */
  private int writePacketHeader(ByteBuffer pkt, int dataLen, int packetLen) {
    pkt.clear();
    // both syncBlock and syncPacket are false
    PacketHeader header = new PacketHeader(packetLen, offset, seqno,
        (dataLen == 0), dataLen, false);

    int size = header.getSerializedSize();
    pkt.position(PacketHeader.PKT_MAX_HEADER_LEN - size);
    header.putInBuffer(pkt);
    return size;
  }

  boolean didSendEntireByteRange() {
    return sentEntireByteRange;
  }

  /**
   * @return the checksum type that will be used with this block transfer.
   */
  DataChecksum getChecksum() {
    return checksum;
  }

  /**
   * @return the offset into the block file where the sender is currently
   *         reading.
   */
  long getOffset() {
    return offset;
  }
}
