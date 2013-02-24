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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.TaskID;
import org.apache.hama.util.KeyValuePair;

public class SortedDiskQueueUsage {

  public static void main(String[] args) {
    TaskAttemptID id = new TaskAttemptID(new TaskID("123", 1, 2), 0);
    Configuration conf = new Configuration();
    SortedDiskQueue q = new SortedDiskQueue();
    q.setConf(conf);
    q.init(conf, id);
    
    q.prepareWrite();
    for (int i = 0; i < 10; i++) {
      q.add((Writable)new KeyValuePair(new Text(i+""), new IntWritable(i)));
    }
    
    q.add(new KeyValuePair(new Text(20+""), new IntWritable(20)));
    q.add(new KeyValuePair(new Text(16+""), new IntWritable(16)));
    
    q.prepareRead();
    
    while(q.size() > 0){
      System.out.println(q.poll());
    }
    
    q.clear();
  }

}
