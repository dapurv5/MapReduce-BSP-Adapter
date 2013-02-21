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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.InputFormat;
import org.apache.hama.bsp.OutputFormat;


public class MapRedBSPJob {
  
  private final static Log LOG = LogFactory.getLog(MapRedBSPJob.class);

  private BSPJob job;
  private HamaConfiguration conf = new HamaConfiguration(new Configuration());
  
  public MapRedBSPJob(){
    try {
      job = new BSPJob(conf);
      
    } catch (IOException e) {
      LOG.error("Cannot instantiate bsp map reduce job. Exiting", e);
      throw new RuntimeException(e);
    }
  }
  
  public void setMapperClass(Class<? extends Mapper<?,?,?,?>> mapperClass){
    job.getConf().set(MapRedBSPConstants.MAPPER_CLASS_NAME, mapperClass.getCanonicalName());
  }
  
  public void setReducerClass(Class<? extends Reducer<?,?,?,?>> reducerClass){
    job.getConf().set(MapRedBSPConstants.REDUCER_CLASS_NAME, reducerClass.getCanonicalName());
  }
  
  public void setMapInputKeyClass(Class<? extends WritableComparable<?>> mapInKeyClass){
    job.getConf().set(MapRedBSPConstants.MAP_IN_KEY_CLASS_NAME, mapInKeyClass.getCanonicalName());
  }
  
  public void setMapInputValueClass(Class<? extends WritableComparable<?>> mapInValClass){
    job.getConf().set(MapRedBSPConstants.MAP_IN_VAL_CLASS_NAME, mapInValClass.getCanonicalName());
  }
  
  public void setMapOutputKeyClass(Class<? extends WritableComparable<?>> mapOutKeyClass){
    job.getConf().set(MapRedBSPConstants.MAP_OUT_KEY_CLASS_NAME, mapOutKeyClass.getCanonicalName());
  }
  
  public void setMapOutputValueClass(Class<? extends Writable> mapOutValClass){
    job.getConf().set(MapRedBSPConstants.MAP_OUT_VAL_CLASS_NAME, mapOutValClass.getCanonicalName());
  }
  
  
  public void setOutputKeyClass(Class<? extends WritableComparable<?>> outKeyClass){
    job.getConf().set(MapRedBSPConstants.REDUCE_OUT_KEY_CLASS_NAME, outKeyClass.getCanonicalName());
    job.setOutputKeyClass(outKeyClass);
  }
  
  public void setOutputValueClass(Class<? extends Writable> outValClass){
    job.getConf().set(MapRedBSPConstants.REDUCE_OUT_VAL_CLASS_NAME, outValClass.getCanonicalName());
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
}
