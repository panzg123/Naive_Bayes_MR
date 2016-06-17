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


import cn.edu.ucas.mapreduce.ModelTrain;
import cn.edu.ucas.mapreduce.PreProb;
import cn.edu.ucas.mapreduce.ModelTrain.ModelTrainMapper;
import cn.edu.ucas.mapreduce.ModelTrain.ModelTrainReducer;
import cn.edu.ucas.mapreduce.PreProb.FileCountMapper;
import cn.edu.ucas.mapreduce.PreProb.FileCountReducer;
import cn.edu.ucas.util.*;

public class TrainWork2 {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: SplitWord <in> <out>");
			System.exit(2);
		}
		conf.set("output", args[1]);
		// 统计分类文档数和总文档数量
		Job job = new Job(conf, "FileCount"); // 设置一个用户定义的job名称
		job.setJarByClass(PreProb.class);
		job.setMapperClass(FileCountMapper.class); // 为job设置Mapper类
		job.setReducerClass(FileCountReducer.class); // 为job设置Reducer类
		job.setOutputKeyClass(Text.class); // 为job的输出数据设置Key类
		job.setOutputValueClass(Text.class); // 为job输出设置value类
		job.setInputFormatClass(TextInputFormat.class);
		String[] strDics = GetAllFilePath.getDic(otherArgs[0], conf);
		String Dic = "";
		for(int i=0;i<strDics.length;i++)
		{
			if(i!=0) Dic += ",";
			Dic +=strDics[i];
		}
		System.out.println(Dic);
	//	FileInputFormat.addInputPaths(job, Dic);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "/fileCount"));// 为job设置输出路径
		// job.setNumReduceTasks(10);
		job.waitForCompletion(true);
		
		// 训练集
		Job job1 = new Job(conf, "splitword"); // 设置一个用户定义的job名称
		job1.setJarByClass(ModelTrain.class);
		job1.setMapperClass(ModelTrainMapper.class); // 为job设置Mapper类
		//此处采用贝努力模型，不许要去重
		// job1.setCombinerClass(ModelTrainCombiner.class);
		job1.setReducerClass(ModelTrainReducer.class); // 为job设置Reducer类
		job1.setOutputKeyClass(Text.class); // 为job的输出数据设置Key类
		job1.setOutputValueClass(Text.class); // 为job输出设置value类
		job1.setInputFormatClass(TextInputFormat.class);
	//	FileInputFormat.addInputPaths(job1, Dic);// 为job设置输入路径
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0])); 
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1] + "/splitWord"));// 为job设置输出路径
		// job1.setNumReduceTasks(10);
		
		job1.waitForCompletion(true);
	}
}