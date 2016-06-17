package cn.edu.ucas.mapreduce;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import cn.edu.ucas.bean.CategoryTotal;
import cn.edu.ucas.util.ReadFileFromHdfs;

public class NativeBayes {
	public static class NativeBayesMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] valueArray = value.toString().split("\t");
			String categoryName = valueArray[0];
			String fileName = valueArray[1];
			String content = valueArray[2];
			StringReader reader = new StringReader(content);
			// 分词NativeBayes
			IKSegmenter iks = new IKSegmenter(reader, true);
			Lexeme lexeme = null;
			while ((lexeme = iks.next()) != null) {
				String out = lexeme.getLexemeText();
				// 过滤掉包含数字和字母的词语
				if (out.charAt(0) >= '0' && out.charAt(0) <= '9' || out.charAt(0) >= 'A' && out.charAt(0) <= 'Z' || out.charAt(0) >= 'a'
						&& out.charAt(0) <= 'z') {
					continue;
				}
				// 输出到Reduce的数据格式为key->value ,word->category@fileName
				context.write(new Text(categoryName + "\t" + fileName), new Text(lexeme.getLexemeText()));
			}
		}
	}

	public static class NativeBayesCombiner extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Set<String> valueSet = new TreeSet<String>();
			
			//tempMap存放 每个单词在文档出现次数
			Map<String, Integer> tempMap = new HashMap<String, Integer>();
			for (Text text : values) {
				if (tempMap.containsKey(text.toString())) {
					int i = tempMap.get(text.toString());
					i++;
					tempMap.put(text.toString(), i);
				} else {
					tempMap.put(text.toString(), 1);
				}
			}
			Set<String> ketSet = tempMap.keySet();
			for (String string : ketSet) {
				int i = tempMap.get(string);
				if (i >= 2) {
					valueSet.add(string);
				}
			}
			for (String text : valueSet) {
				context.write(key, new Text(text));
			}
		}
	}

	public static class NativeBayesPartitoner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			return key.toString().split("\t")[0].hashCode() % numPartitions;
		}

	}

	public static class NativeBayesReducer extends Reducer<Text, Text, Text, Text> {
		List<CategoryTotal> wl = null;
		Map<String, List<CategoryTotal>> tempMap = null;
		List<CategoryTotal> ctl = null;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			wl = ReadFileFromHdfs.getBayesValueFromHdfs(context.getConfiguration().get("output") + "/splitWord/part-r-00000");
			ctl = ReadFileFromHdfs.getCategoryCountList(context.getConfiguration().get("output") + "/fileCount/part-r-00000");
			tempMap = ReadFileFromHdfs.groupList(wl);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			wl = null;
			ctl = null;
			tempMap = null;
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// key为一篇文章的分类和文件名
			// values为该文章分类下的所有词语
			double[] p1 = new double[ctl.size()];
			double[] p2 = new double[ctl.size()];
			double[] p = new double[ctl.size()];
			String[] category = new String[ctl.size()];  //总类数目
			for (Text text : values) {
				List<CategoryTotal> tempList = tempMap.get(text.toString());
				if (tempList == null) {
					continue;
				}
				for (int i = 0; i < p2.length; i++) {
					p2[i] += Math.log((tempList.get(i).getTimes() + 0.8) / (tempList.get(i).getCategoryFileTotal() + 0.8));
					p1[i] = Math.log(tempList.get(i).getCategoryFileTotal() / tempList.get(i).getTotalFile());
					category[i] = tempList.get(i).getCategory();
				}
			}
			for (int i = 0; i < p.length; i++) {
				p[i] = p1[i] + p2[i];
			}
			context.write(key, new Text(category[max(p)] + "\t" + (p1[max(p)] + p2[max(p)])));
		}

		private int max(double[] p) {
			double px = p[0];
			int index = -1;
			for (int i = 1; i < p.length; i++) {
				px = Math.max(px, p[i]);
			}
			for (int i = 0; i < p.length; i++) {
				if (px == p[i]) {
					index = i;
					break;
				}
			}
			return index;
		}
	}
}
