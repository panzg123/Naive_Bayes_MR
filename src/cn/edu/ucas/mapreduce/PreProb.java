package cn.edu.ucas.mapreduce;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

public class PreProb {
	//文档数量Map过程
	public static class FileCountMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context)
				throws java.io.IOException, InterruptedException {
			String[] valueArray = value.toString().split("\t");
			String categoryName = valueArray[0];
			String fileName = valueArray[1];
			String content = valueArray[2];
			StringReader reader = new StringReader(content);
			// 分词
			IKSegmenter iks = new IKSegmenter(reader, true);
			Lexeme lexeme = null;
			int wordcount=0;
			while ((lexeme = iks.next()) != null) {
				String out = lexeme.getLexemeText();
				// 过滤掉包含数字和字母的词语
				if (out.charAt(0) >= '0' && out.charAt(0) <= '9' || out.charAt(0) >= 'A' && out.charAt(0) <= 'Z' || out.charAt(0) >= 'a'
						&& out.charAt(0) <= 'z') {
					continue;
				}
				wordcount++;
				// 输出到Reduce的数据格式为key->value ,word->category\tfileName
				//context.write(new Text(lexeme.getLexemeText()), new Text(categoryName + "\t" + fileName));
			}
			context.write(new Text("category"), new Text(categoryName + "\t" + fileName+"\t"+wordcount));
		}
	}

	public static class FileCountReducer extends Reducer<Text, Text, Text, Text> {
		private String category;
		private String file;
		private String wordcount;
		private Map<String, List<String>> catefileMap = new HashMap<String, List<String>>();
		private Map<String,Integer> wordCountMap = new HashMap<String, Integer>();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// 对每个分类下的文件名进行去重
			List<String> fileList = null;
			for (Text value : values) {
				String[] valuesStr = value.toString().split("\t");
				category = valuesStr[0];
				file = valuesStr[1];
				wordcount = valuesStr[2];
				if (catefileMap.containsKey(category)) {
					
					int temp = wordCountMap.get(category)+Integer.parseInt(wordcount);
					wordCountMap.put(category, temp);
					
					fileList = catefileMap.get(category);
					if (!fileList.contains(file)) {
						fileList.add(file);
					}
					// catefileMap.remove(category);
					// catefileMap.put(category, fileList);
				} else {
					wordCountMap.put(category, Integer.parseInt(wordcount));
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
				context.write(new Text(string), new Text(Integer.toString(catefileMap.get(string).size()) +"\t"+wordCountMap.get(string) + "\t" + totalSize));
			}
		}
	}
}
