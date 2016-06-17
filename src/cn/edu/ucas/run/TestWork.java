package cn.edu.ucas.run;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import cn.edu.ucas.mapreduce.NativeBayes;
import cn.edu.ucas.mapreduce.NativeBayes.NativeBayesCombiner;
import cn.edu.ucas.mapreduce.NativeBayes.NativeBayesMapper;
import cn.edu.ucas.mapreduce.NativeBayes.NativeBayesPartitoner;
import cn.edu.ucas.mapreduce.NativeBayes.NativeBayesReducer;

public class TestWork {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: SplitWord <in> <out>");
			System.exit(2);
		}
		conf.set("output", args[1]);
		// 训练集
		Job job2 = new Job(conf, "test"); // 设置一个用户定义的job名称
		job2.setJarByClass(NativeBayes.class);
		job2.setMapperClass(NativeBayesMapper.class); // 为job设置Mapper类
		job2.setCombinerClass(NativeBayesCombiner.class);
		job2.setPartitionerClass(NativeBayesPartitoner.class);
		job2.setReducerClass(NativeBayesReducer.class); // 为job设置Reducer类
		job2.setOutputKeyClass(Text.class); // 为job的输出数据设置Key类
		job2.setOutputValueClass(Text.class); // 为job输出设置value类
		job2.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job2, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1] + "/Result"));// 为job设置输出路径
		// job2.setNumReduceTasks(10);
		job2.waitForCompletion(true);
	}
}
