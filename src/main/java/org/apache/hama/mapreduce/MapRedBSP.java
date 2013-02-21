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

import org.apache.hama.bsp.message.queue.SortedDiskQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.directmemory.DirectMemory;
import org.apache.directmemory.cache.CacheService;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.message.MessageManager;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.ReflectionUtils;

/**
 * BSP class to emulate a Map-Reduce process.
 */
public class MapRedBSP extends
BSP<WritableComparable<?>, Writable, WritableComparable<?>, Writable, WritableComparable<?>>{

  private final static Log LOG = LogFactory.getLog(MapRedBSP.class);

  private Mapper<WritableComparable<?>, Writable, WritableComparable<?>, Writable> mapper;
  private Reducer<WritableComparable<?>, Writable, WritableComparable<?>, Writable> reducer;
  private WritableComparable<?> mapInKey;
  private Writable              mapInVal;
  private WritableComparable<?> mapOutKey;
  private Writable              mapOutVal;
  private WritableComparable<?> redOutKey;
  private Writable              redOutVal;
  private CacheService<WritableComparable<?>, Writable>  cacheService;

  @SuppressWarnings("unchecked")
  public void setup(
      BSPPeer<WritableComparable<?>, Writable, WritableComparable<?>, Writable, WritableComparable<?>> peer){

    String mapperClassName  = peer.getConfiguration().get(
        MapRedBSPConstants.MAPPER_CLASS_NAME, Mapper.class.getCanonicalName());

    String reducerClassName = peer.getConfiguration().get(
        MapRedBSPConstants.REDUCER_CLASS_NAME, Reducer.class.getCanonicalName());

    String mapInKeyClassName  = peer.getConfiguration().get(MapRedBSPConstants.MAP_IN_KEY_CLASS_NAME);
    String mapInValClassName  = peer.getConfiguration().get(MapRedBSPConstants.MAP_IN_VAL_CLASS_NAME);
    String mapOutKeyClassName = peer.getConfiguration().get(MapRedBSPConstants.MAP_OUT_KEY_CLASS_NAME);
    String mapOutValClassName = peer.getConfiguration().get(MapRedBSPConstants.MAP_OUT_VAL_CLASS_NAME);
    String redOutKeyClassName = peer.getConfiguration().get(MapRedBSPConstants.REDUCE_OUT_KEY_CLASS_NAME);
    String redOutValClassName = peer.getConfiguration().get(MapRedBSPConstants.REDUCE_OUT_VAL_CLASS_NAME);

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

    cacheService = new DirectMemory<WritableComparable<?>, Writable>().
        setNumberOfBuffers(10).
        setSize(1000).
        setInitialCapacity(10000).
        setConcurrencyLevel(4).
        newCacheService();

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
    peer.getConfiguration().set(MessageManager.QUEUE_TYPE_CLASS, SortedDiskQueue.class.getCanonicalName());
  }


  /* (non-Javadoc)
   * @see org.apache.hama.bsp.BSP#bsp(org.apache.hama.bsp.BSPPeer)
   */
  @Override
  public void bsp(
      BSPPeer<WritableComparable<?>, Writable, WritableComparable<?>, Writable, WritableComparable<?>> peer)
          throws IOException, SyncException, InterruptedException {
    //SUPERSTEP-0
    //[MAP AND COMBINE PHASE]
    //    cache = cacheManager.createCache("cache_"+peer.getPeerName());
    Mapper.Context mapperContext = mapper.new Context();

    while(peer.readNext(mapInKey, mapInVal)){
      mapper.map(mapInKey, mapInVal, mapperContext);
    }
  }

  @Override
  public void cleanup(
      BSPPeer<WritableComparable<?>, Writable, WritableComparable<?>, Writable, WritableComparable<?>> peer){
    //    cacheManager.shutdown();
  }
}
