# Hadoop Data Reduction Framework (HDRF)
This is a project for enabling data reduction in HDFS.
New data reduction algorithm can be added through org.apache.hadoop.hdfs.server.datanode.ReductionScheme abstract class and enabled in org.apache.hadoop.hdfs.server.datanode.DataNode #438.

For more details, please check the papers and the poster.

## Papers:
1. [Widodo, R. N. S., Abe, H., & Kato, K. (2020, August). HDRF: Hadoop data reduction framework for hadoop distributed file system. In Proceedings of the 11th ACM SIGOPS Asia-Pacific Workshop on Systems (pp. 122-129).](https://dl.acm.org/doi/abs/10.1145/3409963.3410500)
   This is the base of HDRF, there was no block mirroring, so replication performance is lower.
2. [Widodo, R. N. S., Abe, H., & Kato, K. (2020 November). DFS on a Diet: Enabling Reduction Schemes on Distributed File Systems. SC20 Research Poster.](http://sc20.supercomputing.org/proceedings/tech_poster/poster_files/rpost103s2-file3.pdf) Please also check the [poster](http://sc20.supercomputing.org/proceedings/tech_poster/poster_files/rpost103s2-file2.pdf).
   This is a poster version of #1.
3. [Widodo, R. N. S., Abe, H., & Kato, K. (2021). Hadoop Data Reduction Framework: Applying Data Reduction at the DFS Layer. IEEE Access, 9, 152704-152717.](https://ieeexplore.ieee.org/iel7/6287639/6514899/09612160.pdf)
   This is the complete version of HDRF, which includes reduced Block Mirroring.

# Original README.txt

For the latest information about Hadoop, please visit our website at:

   http://hadoop.apache.org/core/

and our wiki, at:

   http://wiki.apache.org/hadoop/

This distribution includes cryptographic software.  The country in 
which you currently reside may have restrictions on the import, 
possession, use, and/or re-export to another country, of 
encryption software.  BEFORE using any encryption software, please 
check your country's laws, regulations and policies concerning the
import, possession, or use, and re-export of encryption software, to 
see if this is permitted.  See <http://www.wassenaar.org/> for more
information.

The U.S. Government Department of Commerce, Bureau of Industry and
Security (BIS), has classified this software as Export Commodity 
Control Number (ECCN) 5D002.C.1, which includes information security
software using or performing cryptographic functions with asymmetric
algorithms.  The form and manner of this Apache Software Foundation
distribution makes it eligible for export under the License Exception
ENC Technology Software Unrestricted (TSU) exception (see the BIS 
Export Administration Regulations, Section 740.13) for both object 
code and source code.

The following provides more details on the included cryptographic
software:
  Hadoop Core uses the SSL libraries from the Jetty project written 
by mortbay.org.
