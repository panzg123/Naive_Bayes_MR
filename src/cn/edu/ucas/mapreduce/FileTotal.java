package cn.edu.ucas.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class FileTotal {
	//文档数量Map过程
	public static class FileCountMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] valueArray = value.toString().split("\t");
			String categoryName = valueArray[0];   //种类名称
			String fileName = valueArray[1];  //文件名
			context.write(new Text("category"), new Text(categoryName + "\t" + fileName));
		}
	}

	public static class FileCountReducer extends Reducer<Text, Text, Text, Text> {
		private String category;
		private String file;
		private Map<String, List<String>> catefileMap = new HashMap<String, List<String>>();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// 对每个分类下的文件名进行去重
			List<String> fileList = null;
			for (Text value : values) {
				String[] valuesStr = value.toString().split("\t");
				category = valuesStr[0];
				file = valuesStr[1];
				if (catefileMap.containsKey(category)) {
					fileList = catefileMap.get(category);
					if (!fileList.contains(file)) {
						fileList.add(file);
					}
					// catefileMap.remove(category);
					// catefileMap.put(category, fileList);
				} else {
					fileList = new ArrayList<String>();
					fileList.add(file);
					catefileMap.put(category, fileList);
				}
				fileList = null;
			}
			int totalSize = 0;
			Set<String> keyset = catefileMap.keySet();
			for (String string : keyset) {
				totalSize += catefileMap.get(string).size();
			}
			for (String string : keyset) {
				context.write(new Text(string), new Text(Integer.toString(catefileMap.get(string).size()) + "\t" + totalSize));
			}
		}
	}
}
