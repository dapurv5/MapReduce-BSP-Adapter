package org.apache.hama.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Writables {
  
  public static void cloneInto(Writable dest, Writable src) throws IOException{
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(bout);
    src.write(dout);
    bout.close();
    dout.close();
    
    ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
    DataInputStream din = new DataInputStream(bin);
    try{
    dest.readFields(din);
    }
    catch(IOException e){
      if(e instanceof EOFException){
        System.out.println("SOURCE = "+src + " ,DEST = "+dest);
      }
      else{
        throw e; //re-throw the exception
      }
    }
    bin.close();
    din.close();
  }
}
