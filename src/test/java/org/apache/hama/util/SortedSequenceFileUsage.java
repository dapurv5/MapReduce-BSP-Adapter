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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class SortedSequenceFileUsage {

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static void main(String[] args) throws IOException {

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path("/Users/averma/Desktop/Test/sorted.seq");
    
    //TODO: When updating hadoop version. You can add parameters here.
    SortedSequenceFile.Writer writer = SortedSequenceFile.<WritableComparable, Writable>
        createWriter(fs, conf, path, IntWritable.class, Text.class);

    for(int i = 0; i < 20; i++){
      writer.append(new IntWritable(i), new Text("five"));
    }
    for(int i = 30; i > 20; i--){
      writer.append(new IntWritable(i), new Text("five"));
    }
    writer.close();
    
    //Now you can read via an ordinary SequenceFile.Reader instance.

    SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
    IntWritable key = new IntWritable();
    Text value = new Text();
    while(reader.next(key, value)){
      System.out.println(key+"__"+value);
    }
    reader.close();
  }

}
