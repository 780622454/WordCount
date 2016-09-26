/**
 * Created by xiaoqing on 16-10-02.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class MyWordCount {

  public static class FirstMapper
      extends Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    int all = 0;

    public void map(LongWritable key, Text value, Context context
    ) throws IOException, InterruptedException {
      //分词,按行读取,将读取信息按空格分成一个个单词,存储在迭代器中
      StringTokenizer itr = new StringTokenizer(value.toString());
      //遍历迭代器,将一个个单词生成<key, value>键值对
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
        //统计单词总数
        all++;
      }
      if (! itr.hasMoreTokens()){
        //每个mapper任务处理的单词个数输出到自定义的！！allwordcount，由于‘！’ acsii码最小，reduce时候会排到最前面
        context.write(new Text("!!allwordcount"),new IntWritable(all));
      }
    }

  }

  public static class FirstReducer
      extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    //输入格式为<key, 迭代器>,迭代器元素由所有key相同的value组成
    public void reduce(Text key, Iterable<IntWritable> values,
        Context context
    ) throws IOException, InterruptedException {
      int sum = 0;
      //遍历迭代器,累加得到单词出现的次数
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static  class SecondMapper
      extends Mapper<Text, IntWritable, IntWritable, Text>{
    public void map(Text key, IntWritable value, Context context)
        throws IOException, InterruptedException {

//      String val = value.toString().replaceAll("\\t", " ");
//      int index = val.indexOf(" ");
//      String s1 = val.substring(0, index);
//      int s2 = Integer.parseInt(val.substring(index + 1));
//      context.write(new IntWritable(s2), new Text(s1));

      //将key和value互换,以便使用IntWritableDecreasingComparator进行降序排序
      context.write(value,key);
    }
  }

  private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
    public int compare(WritableComparable a, WritableComparable b) {
      return -super.compare(a, b);
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return -super.compare(b1, s1, l1, b2, s2, l2);
    }
  }

  public static class SecondReducer
      extends Reducer <IntWritable,Text,Text,MyValue> {
    float all = 0;

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      MyValue wordvalue = new MyValue();

      for (Text val : values) {
        if (val.toString().startsWith("!!")) {
          all = key.get();
          return;
        }

        Text word = val;

        int count = key.get();
        //tmp 为词频
        float tmp = key.get() / all;
        //将数量和词频存储为自定义MyValue类型
        wordvalue.setMyValue(count, tmp);

        context.write(word, wordvalue);
      }

//      for (Text val : values){
//        // 因为!的ascii最小，map阶段后shuffle排序，!会出现在第一个
//        if (val.toString().startsWith("!!")) {
//          all = Integer.parseInt(key.toString()) ;
//          return;
//        }
//        String key_to = "";
//        key_to += val;
//        //tmp 为词频
//        float tmp = Integer.parseInt(key.toString()) / all;
//        String value = "";
//        value += key.toString()+" ";
//        value += tmp; // 记录词频
//
//        // 将key中单词和文件名进行互换
//        context.write(new Text(key_to), new Text(value));
//      }
    }
  }

  //自定义输出文件名
  private static class MyOut extends TextOutputFormat{

    protected static void setOutputName(JobContext job, String name) {
      job.getConfiguration().set(BASE_OUTPUT_NAME, name);
    }
  }

  public static void main(String[] args) throws Exception {

    //part 1
    //统计单词出现次数，以及单词总数
    Configuration conf1 = new Configuration();
    Job job1 = Job.getInstance(conf1, "word count");
    job1.setJarByClass(MyWordCount.class);
    job1.setMapperClass(FirstMapper.class);
    job1.setCombinerClass(FirstReducer.class);
    job1.setReducerClass(FirstReducer.class);
    job1.setInputFormatClass(TextInputFormat.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    job1.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    job1.waitForCompletion(true);
    //part 2
    //单词排序，计算词频
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "sort");
    job2.setJarByClass(MyWordCount.class);
    job2.setMapperClass(SecondMapper.class);
    job2.setReducerClass(SecondReducer.class);
    job2.setInputFormatClass(SequenceFileInputFormat.class);
    job2.setMapOutputKeyClass(IntWritable.class);
    job2.setMapOutputValueClass(Text.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(MyValue.class);
    job2.setSortComparatorClass(IntWritableDecreasingComparator.class);
    job2.setOutputFormatClass(MyOut.class);
    MyOut.setOutputName(job2, "result.txt");
    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
