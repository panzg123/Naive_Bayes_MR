package cn.edu.ucas.mapreduce;

import java.io.StringReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

public class FenCiMap {
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
			String resultString="";
			while ((lexeme = iks.next()) != null) {
				String out = lexeme.getLexemeText();
				// 过滤掉包含数字和字母的词语
				if (out.charAt(0) >= '0' && out.charAt(0) <= '9' || out.charAt(0) >= 'A' && out.charAt(0) <= 'Z' || out.charAt(0) >= 'a'
						&& out.charAt(0) <= 'z') {
					continue;
				}
				// 输出到Reduce的数据格式为key->value ,word->category\tfileName
				resultString+=out+" ";
				
			}
			context.write(new Text(categoryName), new Text(resultString));
		}
	}
}
