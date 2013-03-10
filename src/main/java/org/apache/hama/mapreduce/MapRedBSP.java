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
import java.util.ArrayList;
import java.util.List;

import org.apache.hama.bsp.message.queue.MemoryQueue;
import org.apache.hama.bsp.message.queue.SortedMessageQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.bsp.message.MessageManager;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.KVPair;
import org.apache.hama.util.ReflectionUtils;
import org.apache.hama.util.SortedSequenceFile;
import org.apache.hama.util.Writables;

/**
 * BSP class to emulate a Map-Reduce process.
 */
public class MapRedBSP 
extends BSP<WritableComparable<?>, Writable, WritableComparable<?>, Writable, KVPair>{

  private final static Log LOG = LogFactory.getLog(MapRedBSP.class);

  private Mapper<WritableComparable<?>, Writable, WritableComparable<?>, Writable> mapper;
  private Reducer<WritableComparable<?>, Writable, WritableComparable<?>, Writable> reducer;
  private WritableComparable<?> mapInKey;
  private Writable              mapInVal;
  private WritableComparable<?> mapOutKey;
  private Writable              mapOutVal;
  private Path                  path;
  private Partitioner<WritableComparable<?>, Writable> partitioner;

  @SuppressWarnings("rawtypes")
  private BSPPeer<WritableComparable<?>, Writable,
  WritableComparable<?>, Writable, KVPair> peer;
  private Configuration conf;

  @SuppressWarnings({ "rawtypes" })
  public void setup(
      BSPPeer<WritableComparable<?>, Writable, WritableComparable<?>, Writable, KVPair> peer){

    this.conf = peer.getConfiguration();
    this.peer = peer;
    this.path = new Path("/tmp/bsp/mapreduce/spills/"+peer.getTaskId()+"/spill_"+"_"+this+peer.getPeerIndex()+".seq");
    doCleanUp(peer);
    String partitionerClassName = conf.get(PARTITIONER_CLASS_NAME);
    try {
      partitioner = ReflectionUtils.newInstance(partitionerClassName);

    } catch (ClassNotFoundException e) {
      LOG.error("Could not initialize partitioner class", e);
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    String mapperClassName  = conf.get(MAPPER_CLASS_NAME,
        Mapper.class.getCanonicalName());

    String reducerClassName = conf.get(REDUCER_CLASS_NAME,
        Reducer.class.getCanonicalName());

    String mapInKeyClassName  = conf.get(MAP_IN_KEY_CLASS_NAME);
    String mapInValClassName  = conf.get(MAP_IN_VAL_CLASS_NAME);
    String mapOutKeyClassName = conf.get(MAP_OUT_KEY_CLASS_NAME);
    String mapOutValClassName = conf.get(MAP_OUT_VAL_CLASS_NAME);

    try {
      mapInKey  = ReflectionUtils.newInstance(mapInKeyClassName);
      mapInVal  = ReflectionUtils.newInstance(mapInValClassName);
      mapOutKey = ReflectionUtils.newInstance(mapOutKeyClassName);
      mapOutVal = ReflectionUtils.newInstance(mapOutValClassName);      

    } catch (ClassNotFoundException e1) {
      LOG.error(e1);
      throw new RuntimeException(e1);
    }   

    try {
      mapper  = ReflectionUtils.newInstance(mapperClassName);
      reducer = ReflectionUtils.newInstance(reducerClassName);

    } catch (ClassNotFoundException e) {
      LOG.error("Could not initialize mapper/reducer Exiting...", e);
      System.exit(-1);
    }
    conf.set(QUEUE_TYPE_CLASS, MemoryQueue.class.getCanonicalName());
  }


  @SuppressWarnings({ "rawtypes" })
  private void doCleanUp(BSPPeer<WritableComparable<?>, Writable,
      WritableComparable<?>, Writable, KVPair> peer){
    try{
      if(peer.getPeerIndex() == 0){
        FileSystem fs = FileSystem.get(peer.getConfiguration());
        if(fs.exists(path)){
          fs.delete(path, true);
        }
      }
    }
    catch(IOException e){
      LOG.info("Unable to cleanup",e);
    }
  }


  /* (non-Javadoc)
   * @see org.apache.hama.bsp.BSP#bsp(org.apache.hama.bsp.BSPPeer)
   */
  @SuppressWarnings({ "rawtypes", "unchecked", "resource" })
  @Override
  public void bsp(
      BSPPeer<WritableComparable<?>, Writable, WritableComparable<?>, Writable, KVPair> peer)
          throws IOException, SyncException, InterruptedException {
    //SUPERSTEP-1
    //[MAP PHASE]
    Mapper.Context mapperContext = mapper.new Context(this);

    while(peer.readNext(mapInKey, mapInVal)){
      mapper.map(mapInKey, mapInVal, mapperContext);
    }

    peer.sync();
    Reducer.Context reducerContext = reducer.new Context(this);

    //SUPERSTEP-2
    //[REDUCE PHASE]
    FileSystem fs = FileSystem.get(conf);
    SortedSequenceFile.Writer writer = SortedSequenceFile.createWriter(fs, conf, path,
        mapOutKey.getClass(), mapOutVal.getClass());
    KVPair msg = null;
    while((msg = peer.getCurrentMessage()) != null){
      writer.append(msg.getKey(), msg.getValue());
    }
    writer.close();

    SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
    List<Writable> valList = new ArrayList<Writable>();

    boolean flag = reader.next(mapOutKey, mapOutVal);
    valList.add(mapOutVal);
    assert(flag == true);

    while(flag){            
      WritableComparable<?> mapOutKeyNxt = ReflectionUtils.newInstance(mapOutKey.getClass());
      Writable mapOutValNxt = ReflectionUtils.newInstance(mapOutVal.getClass());
      flag = reader.next(mapOutKeyNxt, mapOutValNxt);
      if(flag){
        if(mapOutKeyNxt.equals(mapOutKey)){
          valList.add(mapOutValNxt);
        }
        else{
          reducer.reduce(mapOutKey, valList, reducerContext);
          valList = new ArrayList<>();
          mapOutKey = mapOutKeyNxt;
          valList.add(mapOutVal);
        }
      }
    }

    if(valList.size() > 0){
      reducer.reduce(mapOutKey, valList, reducerContext);
    }
  }


  /**
   * Callback from {@link Mapper.Context#write(Object, Object)}
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  protected void mapperContextWrite(WritableComparable<?> key, Writable val){
    int partition = partitioner.getPartition(key, val, peer.getNumPeers());

    try {
      WritableComparable<?> keyCpy = ReflectionUtils.newInstance(
          conf.get(MapRedBSPConstants.MAP_OUT_KEY_CLASS_NAME));
      Writable valCpy = ReflectionUtils.newInstance(
          conf.get(MapRedBSPConstants.MAP_OUT_VAL_CLASS_NAME));

      Writables.cloneInto(keyCpy, key);
      Writables.cloneInto(valCpy, val);

      peer.send(peer.getPeerName(partition),           
          new KVPair(keyCpy, valCpy)); //Reuse KeyValue Pair check here.

    } catch (IOException e) {
      LOG.error("Error sending the message", e);
      e.printStackTrace();

    } catch (ClassNotFoundException e) {
      LOG.error("Error initializing copies");
      e.printStackTrace();
    }
  }

  /**
   * Callback from {@link Reducer.Context#write(Object, Object)}
   */
  protected void reducerContextWrite(WritableComparable<?> key, Writable val){
    try {
      peer.write(key, val);
    } catch (IOException e) {
      LOG.error("Error in writing to fs", e);
      e.printStackTrace();
    }
  }


  @SuppressWarnings("rawtypes")
  @Override
  public void cleanup(
      BSPPeer<WritableComparable<?>, Writable, WritableComparable<?>, Writable, KVPair> peer)
          throws IOException {
    super.cleanup(peer);
  }  
}