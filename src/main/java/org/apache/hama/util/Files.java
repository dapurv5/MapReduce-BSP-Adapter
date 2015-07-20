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
package org.apache.hama.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.util.ReflectionUtils;

public class Files {
  
  private final static Log LOG = LogFactory.getLog(Files.class);

  /**
   * Merges k sequence files each of size n using knlog(k) merge algorithm.
   * @param  inputPath :the input directory which contains sorted sequence
   *                    files, that have to be merged.
   * @param  fs        :the filesystem
   * @return           :the Path to the merged sequence file.
   */
  public static <KEY extends WritableComparable<? super KEY>,VALUE extends Writable>
  Path merge(FileSystem fs, Path inputPath, Class<KEY> keyClazz, Class<VALUE> valClazz){
    Path outputPath = new Path(inputPath.toString()+"/sorted/sorted_file.seq");
    merge(fs, inputPath, outputPath, keyClazz, valClazz);
    return outputPath;
  }

  
  /**
   * Merges k sequence files each of size n using knlog(k) merge algorithm.
   * @param  inputPath :the input directory which contains sorted sequence
   *                    files, that have to be merged.
   * @param  fs        :the filesystem
   * @param outputPath :the path to the merged sorted sequence file.
   */
  public static <KEY extends WritableComparable<? super KEY>,VALUE extends Writable>
      void merge(FileSystem fs, Path inputPath, Path outputPath, Class<KEY> keyClazz, Class<VALUE> valClazz){
        
    Configuration conf = fs.getConf();
    
    PriorityQueue<KVPair<KEY,VALUE>> pq = new 
        PriorityQueue<KVPair<KEY, VALUE>>();
    
    //Map from KeyValuePair to the split number to which it belongs.
    HashMap<KVPair<KEY, VALUE>, Integer> keySplitMap = new 
        HashMap<KVPair<KEY, VALUE>, Integer>();
    
    FileStatus[] files;
    SequenceFile.Writer writer = null;
    SequenceFile.Reader reader[] = null;    
    try {
      files = fs.listStatus(inputPath);
      reader = new SequenceFile.Reader[files.length];
      
      for (int i = 0; i < files.length; i++) {
        if(files[i].getLen() > 0){
          reader[i] = new SequenceFile.Reader(fs, files[i].getPath(),conf);          
          KEY key   = ReflectionUtils.newInstance(keyClazz, new Object[0]);
          VALUE val = ReflectionUtils.newInstance(valClazz, new Object[0]);

            reader[i].next(key, val);
            KVPair<KEY,VALUE> kv = new KVPair<KEY,VALUE>(key,val); 
            pq.add(kv);
            keySplitMap.put(kv, i);
        }
      }
      
      writer = SequenceFile.createWriter(fs, conf, 
          outputPath, keyClazz, valClazz);
      
      while(!pq.isEmpty()){
        KVPair<KEY, VALUE> smallestKey = pq.poll();
        writer.append(smallestKey.getKey(), smallestKey.getValue());
        Integer index = keySplitMap.get(smallestKey);        
        keySplitMap.remove(smallestKey);
        
        KEY key   = ReflectionUtils.newInstance(keyClazz, new Object[0]);
        VALUE val = ReflectionUtils.newInstance(valClazz, new Object[0]);
                
        if(reader[index].next(key, val)){
          KVPair<KEY,VALUE> kv = new KVPair<KEY,VALUE>(key,val); 
          pq.add(kv);
          keySplitMap.put(kv, index);
        }        
      }

    } catch (IOException e) {
      LOG.error("Couldn't get status, exiting ...", e);
      System.exit(-1);
    }
    finally{
      if(writer != null){
        try {
          writer.close();
        } catch (IOException e) {
          LOG.error("Cannot close writer to sorted seq. file. Exiting ...", e);
          System.exit(-1);
        }
      }
    }
  }
}