/**
 *  Copyright (c) 2011 Apurv Verma
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.base.Objects;

/**
 * Container to hold a KeyValuePair.
 * Note: Hashcode is content independent.
 * <code>
 *  KeyValuePair<Text, IntWritable> kv1 = new 
 *      KeyValuePair<Text, IntWritable>(new Text("a"), new IntWritable(1));
 *  
 *  KeyValuePair<Text, IntWritable> kv2 = new 
 *      KeyValuePair<Text, IntWritable>(new Text("a"), new IntWritable(1));
 *  
 *  System.out.println(kv1.hashCode()); //Both hashcodes will be different.
 *  System.out.println(kv2.hashCode());
 *  
 * </code>
 */
public class KeyValuePair
<KEY extends WritableComparable<? super KEY>,VALUE extends Writable> 
implements WritableComparable<KeyValuePair<KEY,VALUE>>{
  
  private KEY key;
  private VALUE val;
  
  public KeyValuePair(){}
    
  public KeyValuePair(KEY key, VALUE val){
    this.key = key;
    this.val = val;
  }
  
  public KEY getKey() {
    return key;
  }

  public void setKey(KEY key) {
    this.key = key;
  }

  public VALUE getValue() {
    return val;
  }

  public void setValue(VALUE val) {
    this.val = val;
  }
  
  @Override
  public boolean equals(Object obj){
    if(this == obj)
      return true;
    if(null == obj)
      return false;
    if(getClass() != obj.getClass())
      return false;
    @SuppressWarnings("unchecked")
    KeyValuePair<KEY, VALUE> that = (KeyValuePair<KEY, VALUE>)obj;
    return Objects.equal(this.getKey(), that.getKey())
        && Objects.equal(this.getValue(), that.getValue());
  }

  @Override
  public String toString(){
    return Objects.toStringHelper(this)
        .add("Key", getKey())
        .add("Value", getValue())
        .toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(getKey().getClass().getCanonicalName());
    getKey().write(out);
    out.writeUTF(getValue().getClass().getCanonicalName());
    getValue().write(out);
  }

  
  @SuppressWarnings("unchecked")
  @Override
  public void readFields(DataInput in) throws IOException {
    String firstClass = in.readUTF();
    try {
      setKey((KEY)ReflectionUtils.newInstance(Class.forName(
          firstClass), null));      
      getKey().readFields(in);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }    
    
    String secondClass = in.readUTF();
    try {
      setValue((VALUE)ReflectionUtils.newInstance(Class.forName(
          secondClass), null));
      getValue().readFields(in);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }
  
  /* (non-Javadoc)
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(KeyValuePair<KEY, VALUE> that) {
    return getKey().compareTo(that.getKey());
  }

  
  public static void main(String[] args){
    KeyValuePair<Text, IntWritable> kv1 = new 
        KeyValuePair<Text, IntWritable>(new Text("a"), new IntWritable(1));
    
    KeyValuePair<Text, IntWritable> kv2 = new 
        KeyValuePair<Text, IntWritable>(new Text("a"), new IntWritable(1));
    
    System.out.println(kv1.hashCode());
    System.out.println(kv2.hashCode());
    System.out.println(kv1.compareTo(kv2));
    System.out.println(kv1.equals(kv2));
  }
}