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

/**
 * Stores various constants.
 */
public interface MapRedBSPConstants {

  //Configuration Keys
  public static final String MAPPER_CLASS_NAME         = "hama.mapreduce.mapper.class";
  public static final String REDUCER_CLASS_NAME        = "hama.mapreduce.reducer.class";
  public static final String MAP_IN_KEY_CLASS_NAME     = "hama.mapreduce.map.key.input.class";
  public static final String MAP_OUT_KEY_CLASS_NAME    = "hama.mapreduce.map.key.output.class";
  public static final String MAP_IN_VAL_CLASS_NAME     = "hama.mapreduce.map.value.input.class";
  public static final String MAP_OUT_VAL_CLASS_NAME    = "hama.mapreduce.map.value.output.class";
  public static final String REDUCE_OUT_KEY_CLASS_NAME = "hama.mapreduce.reduce.key.output.class";
  public static final String REDUCE_OUT_VAL_CLASS_NAME = "hama.mapreduce.reduce.value.output.class";
  
  //Cache Constants
  public static final int    CACHE_SIZE              = 1000;
}
