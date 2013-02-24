/**
 *  Copyright (c) 2012 Apurv Verma
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *  
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *  
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE. 
 *  limitations under the License.
 */
package org.apache.hama.mapreduce;

import static org.apache.hama.mapreduce.MapRedBSPConstants.*;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.InputFormat;
import org.apache.hama.bsp.OutputFormat;


public class MapRedBSPJob{
  
  private final static Log LOG = LogFactory.getLog(MapRedBSPJob.class);

  private BSPJob job;
  private HamaConfiguration conf = new HamaConfiguration(new Configuration());
  
  public MapRedBSPJob(){
    try {
      job = new BSPJob(conf);
      job.setBspClass(MapRedBSP.class);
      job.setNumBspTask(1);
      
    } catch (IOException e) {
      LOG.error("Cannot instantiate bsp map reduce job. Exiting", e);
      throw new RuntimeException(e);
    }
  }
  
  public void setMapperClass(Class mapperClass){
    conf.set(MAPPER_CLASS_NAME, mapperClass.getName());
  }
  
  public void setReducerClass(Class reducerClass){
    conf.set(REDUCER_CLASS_NAME, reducerClass.getName());
  }
  
  @SuppressWarnings("rawtypes")
  public void setMapInputKeyClass(Class mapInKeyClass){
    conf.set(MAP_IN_KEY_CLASS_NAME, mapInKeyClass.getName());
    job.setInputKeyClass(mapInKeyClass);
  }
  
  @SuppressWarnings("rawtypes")
  public void setMapInputValueClass(Class mapInValClass){
    conf.set(MAP_IN_VAL_CLASS_NAME, mapInValClass.getName());
    job.setInputValueClass(mapInValClass);
  }
  
  @SuppressWarnings("rawtypes")
  public void setMapOutputKeyClass(Class mapOutKeyClass){
    conf.set(MAP_OUT_KEY_CLASS_NAME, mapOutKeyClass.getName());
  }
  
  @SuppressWarnings("rawtypes")
  public void setMapOutputValueClass(Class mapOutValClass){
    conf.set(MAP_OUT_VAL_CLASS_NAME, mapOutValClass.getName());
  }
    
  public void setReduceOutputKeyClass(Class outKeyClass){
    conf.set(REDUCE_OUT_KEY_CLASS_NAME, outKeyClass.getName());
    job.setOutputKeyClass(outKeyClass);
  }
  
  public void setReduceOutputValueClass(Class outValClass){
    conf.set(REDUCE_OUT_VAL_CLASS_NAME, outValClass.getName());
    job.setOutputValueClass(outValClass);
  }
  
  public void setInputFormat(Class<? extends InputFormat> inputFormat){
    job.setInputFormat(inputFormat);
  }
  
  public void setOutputFormat(Class<? extends OutputFormat> outputFormat){
    job.setOutputFormat(outputFormat);
  }
  
  public void setJobName(String name){
    job.setJobName(name);
  }
  
  @SuppressWarnings("rawtypes")
  public void setPartitionerClass(Class partitionerClass){
    conf.set(PARTITIONER_CLASS_NAME, partitionerClass.getName());
  }
  
  public void waitForCompletion(boolean verbose) 
      throws IOException, InterruptedException, ClassNotFoundException{
    job.waitForCompletion(verbose);
  }
  
  public void setInputPath(Path path){
    job.setInputPath(path);
  }
  
  public void setOutputPath(Path path){
    job.setOutputPath(path);
  }
  
  public void setJarByClass(Class<?> cls){
    job.setJarByClass(cls);
  }
  
  
}