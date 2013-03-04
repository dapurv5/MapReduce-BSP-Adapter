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

import java.io.Closeable;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.TreeMultiset;

/**
 * Different from MapFile as values need not necessarily be written
 * in sorted order and yet the resulting SequenceFile is guaranteed
 * to be sorted.
 * 
 * Can set configuration parameter "spill.size" which specified the size
 * of each spill.
 * Uses "/tmp/spills/" as a temporary storage.
 */
public class SortedSequenceFile{

  private static final String KEY_VALUES_PER_SPILL_CONF = "spill.size";
  private static final Log LOG = LogFactory.getLog(SortedSequenceFile.class);

  public static class Writer
  <KEY extends WritableComparable<? super KEY>,VALUE extends Writable>
  implements Closeable{

    private FileSystem fs;
    private Configuration conf;
    private Path path; //the path to the sorted seq. file that is to be written
    private final int SPILL_SIZE;
    private TreeMultiset<KVPair<KEY, VALUE>> spill;
    private int SPILL_SEQ_NUMBER = 0;
    private Class<KEY> keyClass;
    private Class<VALUE> valClass;

    private static int counter = -1;

    private Writer(FileSystem fs, Configuration conf, Path path, Class<KEY> keyClass,
        Class<VALUE> valClass){
      this.fs       = fs;
      this.conf     = conf;
      this.path     = path;
      this.keyClass = keyClass;
      this.valClass = valClass;
      this.spill    = TreeMultiset.create(); 
      SPILL_SIZE    = Integer.parseInt(conf.get(KEY_VALUES_PER_SPILL_CONF,"100"));
      counter++;
    }

    public void append(KEY key, VALUE val){
      KEY keyCpy = ReflectionUtils.newInstance(keyClass);
      VALUE valCpy = ReflectionUtils.newInstance(valClass);
      try {
        WritableUtils.cloneInto(keyCpy, key);
        WritableUtils.cloneInto(valCpy, val);

      } catch (IOException e) {
        LOG.error("Error buffering msgs", e);
        e.printStackTrace();
      }
      spill.add(new KVPair<KEY, VALUE>(keyCpy, valCpy));
      if(spill.size() == SPILL_SIZE){
        spillToDisk();
      }
    }

    public void spillToDisk(){
      Path tmpPath = new Path(getSpillDir() +"/spill_"+SPILL_SEQ_NUMBER);
      SPILL_SEQ_NUMBER++;
      SequenceFile.Writer writer = null;      
      try {
        writer = SequenceFile.createWriter(fs, conf,tmpPath, keyClass,valClass);
        for(KVPair<KEY, VALUE> kv:spill){
          writer.append(kv, NullWritable.get());
        }
        spill = TreeMultiset.create();
      } catch (IOException e) {
        LOG.error("Problem in writing. Exiting...", e);
        System.exit(-1);
      }
      finally{
        if(writer != null){
          try {
            writer.close();
          } catch (IOException e) {
            LOG.error("Could not close writer", e);
          }
        }
      }      
    }

    /* (non-Javadoc)
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
      if(spill.size() > 0){
        spillToDisk();
      }
      Path inputPath = new Path(getSpillDir());
      Files.<KEY, VALUE>merge(fs, inputPath, path, keyClass, valClass);
      fs.delete(inputPath, true);
    }

    private String getSpillDir(){
      return "/tmp/spills/"+Thread.currentThread().getId()+"_"+counter;
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static
  <KEY extends WritableComparable<? super KEY>, VALUE extends Writable>
  Writer createWriter(FileSystem fs, Configuration conf, Path path,
      Class keyClass, Class valClass){    
    return new Writer(fs, conf, path, keyClass, valClass);
  }  
}