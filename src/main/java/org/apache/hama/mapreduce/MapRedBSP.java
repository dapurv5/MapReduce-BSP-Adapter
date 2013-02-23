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

import org.apache.hama.bsp.message.queue.SortedDiskQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.message.MessageManager;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.KeyValuePair;
import org.apache.hama.util.ReflectionUtils;

/**
 * BSP class to emulate a Map-Reduce process.
 */
public class MapRedBSP 
extends BSP<WritableComparable<?>, Writable, WritableComparable<?>, Writable, WritableComparable<?>>{

  private final static Log LOG = LogFactory.getLog(MapRedBSP.class);

  private Mapper<WritableComparable<?>, Writable, WritableComparable<?>, Writable> mapper;
  private Reducer<WritableComparable<?>, Writable, WritableComparable<?>, Writable> reducer;
  private WritableComparable<?> mapInKey;
  private Writable              mapInVal;
  private WritableComparable<?> mapOutKey;
  private Writable              mapOutVal;
  private WritableComparable<?> redOutKey;
  private Writable              redOutVal;

  private BSPPeer<WritableComparable<?>, Writable,
  WritableComparable<?>, Writable, WritableComparable<?>> peer;
  private Configuration conf;

  @SuppressWarnings("unchecked")
  public void setup(
      BSPPeer<WritableComparable<?>, Writable, WritableComparable<?>, Writable, WritableComparable<?>> peer){

    this.conf = peer.getConfiguration();
    this.peer = peer;

    String mapperClassName  = conf.get(MAPPER_CLASS_NAME,
        Mapper.class.getCanonicalName());

    String reducerClassName = conf.get(REDUCER_CLASS_NAME,
        Reducer.class.getCanonicalName());

    String mapInKeyClassName  = conf.get(MAP_IN_KEY_CLASS_NAME);
    String mapInValClassName  = conf.get(MAP_IN_VAL_CLASS_NAME);
    String mapOutKeyClassName = conf.get(MAP_OUT_KEY_CLASS_NAME);
    String mapOutValClassName = conf.get(MAP_OUT_VAL_CLASS_NAME);
    String redOutKeyClassName = conf.get(REDUCE_OUT_KEY_CLASS_NAME);
    String redOutValClassName = conf.get(REDUCE_OUT_VAL_CLASS_NAME);

    try {
      mapInKey  = ReflectionUtils.newInstance(mapInKeyClassName);
      mapInVal  = ReflectionUtils.newInstance(mapInValClassName);
      mapOutKey = ReflectionUtils.newInstance(mapOutKeyClassName);
      mapOutVal = ReflectionUtils.newInstance(mapOutValClassName);
      redOutKey = ReflectionUtils.newInstance(redOutKeyClassName);
      redOutVal = ReflectionUtils.newInstance(redOutValClassName);

    } catch (ClassNotFoundException e1) {
      LOG.error(e1);
      throw new RuntimeException(e1);
    }

    try {
      mapper  = (Mapper<WritableComparable<?>, Writable, WritableComparable<?>, Writable>)
          ReflectionUtils.newInstance(mapperClassName);
      reducer = (Reducer<WritableComparable<?>, Writable, WritableComparable<?>, Writable>)
          ReflectionUtils.newInstance(reducerClassName);

    } catch (ClassNotFoundException e) {
      LOG.error("Could not initialize mapper/reducer Exiting...", e);
      System.exit(-1);
    }

    //Use SortedDiskQueue (TODO: Come up with a faster queue implementation)
    //TODO: Change hama code, there's no need to use a SortedDiskQueue in superstep 0.
    conf.set(MessageManager.QUEUE_TYPE_CLASS, SortedDiskQueue.class.getCanonicalName());
  }


  /* (non-Javadoc)
   * @see org.apache.hama.bsp.BSP#bsp(org.apache.hama.bsp.BSPPeer)
   */
  @Override
  public void bsp(
      BSPPeer<WritableComparable<?>, Writable, WritableComparable<?>, Writable, WritableComparable<?>> peer)
          throws IOException, SyncException, InterruptedException {
    //SUPERSTEP-0
    //[MAP PHASE]
    Mapper.Context mapperContext = mapper.new Context(this);

    while(peer.readNext(mapInKey, mapInVal)){
      mapper.map(mapInKey, mapInVal, mapperContext);
    }

    peer.sync();
    
    Reducer.Context reducerContext = reducer.new Context(this);

    //SUPERSTEP-1
    //[REDUCE PHASE]    
    List<Writable> valList = new ArrayList<Writable>();
    KeyValuePair msg = (KeyValuePair) peer.getCurrentMessage();
    boolean flag = (msg != null);
    assert(flag == true);
    mapOutKey = msg.getKey();
    mapOutVal = msg.getValue();
    
    while(flag){
      WritableComparable<?> mapOutKeyNxt = ReflectionUtils.
          newInstance(mapOutKey.getClass());
      Writable mapOutValNxt = ReflectionUtils.
          newInstance(mapOutVal.getClass());
      msg = (KeyValuePair) peer.getCurrentMessage();
      flag = (msg != null);
      if(flag){
        if(mapOutKeyNxt.equals(mapOutKey)){
          valList.add(mapOutValNxt);
        }
        else{
          reducer.reduce(mapOutKey, valList, reducerContext);
        }
      }
    }
  }

  @Override
  public void cleanup(
      BSPPeer<WritableComparable<?>, Writable, WritableComparable<?>, Writable, WritableComparable<?>> peer){
    
  }

  /**
   * Callback from {@link Mapper.Context#write(Object, Object)}
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  protected void mapperContextWrite(WritableComparable<?> key, Writable val){
    String partitionerClassName = conf.get(PARTITIONER_CLASS_NAME);
    Partitioner<WritableComparable<?>, Writable> partitioner = null;
    try {
      partitioner = ReflectionUtils.newInstance(partitionerClassName);

    } catch (ClassNotFoundException e) {
      LOG.error("Could not initialize partitioner class", e);
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    int partition = partitioner.getPartition(key, val, peer.getNumPeers());
    try {
      peer.send(peer.getPeerName(partition),
          new KeyValuePair(key, val));

    } catch (IOException e) {
      LOG.error("Error sending the message", e);
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
}
