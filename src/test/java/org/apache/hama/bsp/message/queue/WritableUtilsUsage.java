package org.apache.hama.bsp.message.queue;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hama.util.KVPair;
import org.apache.hama.util.ReflectionUtils;

public class WritableUtilsUsage {

  @SuppressWarnings({ "rawtypes", "deprecation", "unchecked" })
  public static void main(String[] args) throws IOException, ClassNotFoundException {
    
    KVPair kv = new KVPair(new Text("one"), new IntWritable(1));
    KVPair kvCpy = new KVPair();
    WritableUtils.cloneInto(kvCpy, kv);
    
    System.out.println(kv + "  __ "+kvCpy);
    
    //You could also use canonical name here.
    Writable key = ReflectionUtils.newInstance(KVPair.class.getName());
    WritableUtils.cloneInto(key, kv);
    
    System.out.println(key);
    
    Writable nullVal = ReflectionUtils.newInstance(NullWritable.class);
    WritableUtils.cloneInto(nullVal, NullWritable.get());
    System.out.println(nullVal);
  }

}
