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
package org.apache.hama.bsp.message.queue;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.message.queue.MessageQueue;
import org.apache.hama.util.KVPair;
import org.apache.hama.util.ReflectionUtils;
import org.apache.hama.util.SortedSequenceFile;

/**
 * A disk based queue that is backed by a raw file on local disk. <br/>
 * Structure is as follows: <br/>
 * If "bsp.disk.queue.dir" is not defined, "hama.tmp.dir" will be used instead. <br/>
 * ${hama.tmp.dir}/diskqueue/job_id/task_attempt_id/ <br/>
 * 
 * It is recommended to use the file:// scheme in front of the property, because
 * writes on DFS are expensive, however your local disk may not have enough
 * space for your message, so you can easily switch per job via your
 * configuration. <br/>
 * 
 * The job_id dir will never be deleted. So you need a cronjob to do the
 * cleanup for you. <br/>
 */ 
public class SortedDiskQueue<M extends Writable> implements MessageQueue<M>
, MessageTransferQueue<M>{

  public static final String DISK_QUEUE_PATH_KEY = "bsp.disk.queue.dir";
  private static final int MAX_RETRIES = 4;
  private static final Log LOG = LogFactory.getLog(SortedDiskQueue.class);

  private static int counter = -1;

  private int size = 0;
  private Configuration conf;
  private FileSystem fs;

  @SuppressWarnings("rawtypes")
  private SortedSequenceFile.Writer writer;
  private SequenceFile.Reader reader;

  private Path queuePath;  
  private TaskAttemptID id;

  public SortedDiskQueue(){
    counter++;
  }


  /* (non-Javadoc)
   * @see org.apache.hama.bsp.message.MessageQueue#add(java.lang.Object)
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void add(M elem) {
    if(size == 0){
      writer = SortedSequenceFile.<WritableComparable, Writable>createWriter(
          fs, conf, queuePath, elem.getClass(), NullWritable.class);
      System.out.println("opening a writer for "+queuePath);////////////////
    }
    size++;
    writer.append((WritableComparable<?>) elem, NullWritable.get());
  }

  /* (non-Javadoc)
   * @see org.apache.hama.bsp.message.MessageQueue#addAll(java.util.Collection)
   */
  @Override
  public void addAll(Collection<M> collection) {    
    for(M item:collection){
      add(item);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hama.bsp.message.MessageQueue#addAll(org.apache.hama.bsp.message.MessageQueue)
   */
  @Override
  public void addAll(MessageQueue<M> otherQueue) {
    M m = null;
    while((m = otherQueue.poll()) != null){
      add(m);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hama.bsp.message.MessageQueue#clear()
   */
  @Override
  public void clear() {
    size = 0;
    try {
      System.out.println("Clearing i.e. closing the writer at "+queuePath);
      writer.close();
      close();
    } catch (IOException e) {
      LOG.error("Cannot clear disk", e);
    }
    init(conf, id);
  }

  /* (non-Javadoc)
   * @see org.apache.hama.bsp.message.MessageQueue#close()
   */
  @Override
  public void close() {    
    try {
      System.out.println("Closing the SortedDiskQueue and reader.close");
      reader.close();
      fs.delete(queuePath, true);
    } catch (IOException e) {
      LOG.error("Cannot close disk queue", e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.conf.Configurable#getConf()
   */
  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Finds and returns the path on the disk to be used for backing 
   * up the contents of the sorted disk queue.
   * 
   * @return the path to the directory containing the backup
   */
  private Path getQueueDir(){   
    String queueDir = conf.get(DISK_QUEUE_PATH_KEY);    
    if(queueDir == null){
      if(conf.get("hama.tmp.dir") == null){
        queueDir = "/tmp/bsp_message_store";
      }
      else{
        queueDir = conf.get("hama.tmp.dir");        
      }
    }    
    queueDir += "/sorted_disk_queue/" + id.getJobID().toString() 
        +"/"+id.getTaskID()+"_"+counter+".seq";
    return new Path(queueDir);
  }

  @Override
  public MessageQueue<M> getReceiverQueue() {
    return this;
  }

  @Override
  public MessageQueue<M> getSenderQueue() {
    return this;
  }

  /* (non-Javadoc)
   * @see org.apache.hama.bsp.message.MessageQueue#init(org.apache.hadoop.conf.Configuration, org.apache.hama.bsp.TaskAttemptID)
   */
  @Override
  public void init(Configuration conf, TaskAttemptID id) {
    this.id   = id;
    this.conf = conf;    
    try {
      fs = FileSystem.get(conf);
      queuePath = getQueueDir();

    } catch (IOException e) {
      LOG.error("Error in initializing the Sorted Disk Queue...", e);
      throw new RuntimeException(e); //Can't recover
    }
    prepareWrite();
  }

  /* (non-Javadoc)
   * @see org.apache.hama.bsp.message.queue.MessageQueue#isMessageSerialized()
   */
  @Override
  public boolean isMessageSerialized() {
    return false;
  }

  /* (non-Javadoc)
   * @see java.lang.Iterable#iterator()
   */
  @Override
  public Iterator<M> iterator() {
    Iterator<M> iter = new Iterator<M>(){

      @Override
      public boolean hasNext() {
        return SortedDiskQueue.this.size() > 0;
      }

      @Override
      public M next() {
        if(SortedDiskQueue.this.size() == 0){
          SortedDiskQueue.this.close();
        }
        return SortedDiskQueue.this.poll();
      }

      @Override
      public void remove() {
        // NO OP
      }

    };
    prepareRead();
    return iter;
  }

  /* (non-Javadoc)
   * @see org.apache.hama.bsp.message.MessageQueue#poll()
   */
  @Override
  public M poll() {
    if(size == 0){
      return null;
    }
    size--;
    for(int tries = 0; tries < MAX_RETRIES; tries++){      
      try {
        KVPair kv = new KVPair();
        System.out.println("called reader.next()");
        reader.next(kv, NullWritable.get());
        System.out.println("returned reader.next()");

        //        System.out.println("Polling to q elem="+kv);///////

        return (M) kv;
      } catch (IOException e) {
        LOG.error("Retrying for the " + tries + "th time!", e);
      }     
    }
    throw new RuntimeException("Couldn't poll from disk. Exiting...");
  }

  /* (non-Javadoc)
   * @see org.apache.hama.bsp.message.MessageQueue#prepareRead()
   */
  @Override
  public void prepareRead() {
    System.err.println("Prepare read() called \n\n");
    try {
      if(writer != null){
        writer.close(); 
      }
      if(fs.exists(queuePath)){
        reader = new SequenceFile.Reader(fs, queuePath, conf);
      }
    } catch (IOException e) {
      LOG.error("Cannot prepare to read",e);
      throw new RuntimeException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hama.bsp.message.MessageQueue#prepareWrite()
   */
  @Override
  public void prepareWrite() {
    // Handled in add()    
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /* (non-Javadoc)
   * @see org.apache.hama.bsp.message.MessageQueue#size()
   */
  @Override
  public int size() {
    return size;
  }
}