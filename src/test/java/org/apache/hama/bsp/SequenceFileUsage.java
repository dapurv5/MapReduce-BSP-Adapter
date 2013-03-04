package org.apache.hama.bsp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hama.util.KVPair;

public class SequenceFileUsage {

  /**
   * Checking the reading and writing of custom writables from a sequence file.
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Class keyClass = KVPair.class;
    Class valClass = NullWritable.class;
    Path name = new Path("/tmp/seqfiletest.seq");
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, name, keyClass, valClass);
    Text key = new Text("Apurv");
    IntWritable val = new IntWritable(1);
    KVPair kvPair = new KVPair(key, val);
    writer.append(kvPair , NullWritable.get());
    
    KVPair kvPairRead = new KVPair();
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, name, conf);
    reader.next(kvPairRead, NullWritable.get());
    System.out.println(kvPairRead);
    reader.close();
  }
}