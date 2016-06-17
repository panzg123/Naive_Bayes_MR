package cn.edu.ucas.mapreduce;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import cn.edu.ucas.bean.CategoryTotal;
import cn.edu.ucas.util.ReadFileFromHdfs;

public class ModelTrain {
	public static class ModelTrainMapper extends Mapper<LongWritable, Text, Text, Text> {
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
			while ((lexeme = iks.next()) != null) {
				String out = lexeme.getLexemeText();
				// 过滤掉包含数字和字母的词语
				if (out.charAt(0) >= '0' && out.charAt(0) <= '9' || out.charAt(0) >= 'A' && out.charAt(0) <= 'Z' || out.charAt(0) >= 'a'
						&& out.charAt(0) <= 'z') {
					continue;
				}
				// 输出到Reduce的数据格式为key->value ,word->category\tfileName
				context.write(new Text(lexeme.getLexemeText()), new Text(categoryName + "\t" + fileName));
			}
		}
	}

	/*
	 * Combiner过程
	 */
	public static class ModelTrainCombiner extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// 对同一个单词在同一个分类的同一篇文章中出现的次数去重
			// 即处理词频
			Set<Text> valuesSet = new TreeSet<Text>();
			for (Text text : values) {
				valuesSet.add(text);
			}
			for (Text text : valuesSet) {
				context.write(key, text);
			}
		}
	}

	public static class ModelTrainReducer extends Reducer<Text, Text, Text, Text> {
		List<CategoryTotal> fileList = null;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			//获取文档统计数据
			fileList = ReadFileFromHdfs.getCategoryCountList(context.getConfiguration().get("output") + "/fileCount/part-r-00000");
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			fileList = null;
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<String, List<String>> categoryFileKeyValue = new HashMap<String, List<String>>();
			Set<String> tempSet = new TreeSet<String>();
			for (Text text : values) {
				tempSet.add(text.toString());
			}
			List<String> valueList = null;
			for (String text : tempSet) {
				String[] valueArray = text.split("\t");
				if (valueArray.length != 2) {
					continue;
				}
				// 计算分类下面的文件数量如果分类不存在那就表示该单词在该分类下的0个文档中出现
				// valueArray[0]为分类,valueArray[1]为文件名
				String category = valueArray[0];
				String fileName = valueArray[1];
				if (categoryFileKeyValue.containsKey(category)) {
					valueList = categoryFileKeyValue.get(category);
					if (!valueList.contains(fileName)) {
						valueList.add(fileName);
					}
					// categoryFileKeyValue.remove(category);
					// categoryFileKeyValue.put(category, valueList);
				} else {
					valueList = new ArrayList<String>();
					valueList.add(fileName);
					categoryFileKeyValue.put(category, valueList);
				}
				valueList = null;
			}
			for (CategoryTotal ct : fileList) {
				if (categoryFileKeyValue.get(ct.getCategory()) == null) {
					context.write(key, new Text(ct.getCategory() + "\t0\t" + ct.getCategoryFileTotal() + "\t" + ct.getTotalFile()));
				} else {
					context.write(key,
							new Text(ct.getCategory() + "\t" + categoryFileKeyValue.get(ct.getCategory()).size() + "\t" + ct.getCategoryFileTotal()
									+ "\t" + ct.getTotalFile()));
				}
			}
		}
	}
}
