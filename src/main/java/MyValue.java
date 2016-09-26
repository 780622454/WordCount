import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义value类型,为单词数量和词频
 *
 * @author wangzhe
 * @create 2016-09-25-10:34
 */

public class MyValue implements Writable{
  //单词出现次数
  private int wordcount;
  //词频比率
  private float frequency;

  public void write(DataOutput out) throws IOException {
    out.writeInt(wordcount);
    out.writeFloat(frequency);
  }

  public void readFields(DataInput in) throws IOException {
    wordcount = in.readInt();
    frequency = in.readFloat();
  }

  public void setMyValue(int count, float freq){
    this.wordcount = count;
    this.frequency = freq;
  }

  //用于TextOutputFormat输出类型在文本文件中打印value
  public String toString(){
    return Integer.toString(wordcount) + " " + Float.toString(frequency);
  }
}
