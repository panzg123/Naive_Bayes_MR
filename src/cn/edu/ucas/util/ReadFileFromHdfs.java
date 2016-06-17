package cn.edu.ucas.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import cn.edu.ucas.bean.CategoryTotal;

/**
 * 
 * @author cqyyjdw
 * @date 2014年5月13日
 * @version 0.0.1
 * @function 读取hdfs上指定的文件
 */
public class ReadFileFromHdfs {
	/**
	 * 根据hdfs文件路径来获取hdfs上文件内容保存到list集合中，内容为分类，分类下文档总数，训练集文档总数
	 * 
	 * @param context
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	public static List<CategoryTotal> getCategoryCountList(String filePath) throws IOException {
		List<CategoryTotal> resultList = new ArrayList<CategoryTotal>();
		String uri = filePath;
		Configuration conf = new Configuration();
		conf.set("hadoop.job.ugi", "hadoop-user,hadoop-user");
		// FileSystem是用户操作HDFS的核心类，它获得URI对应的HDFS文件系统
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream fin = null;
		BufferedReader in = null;
		try {
			// 让FileSystem打开一个uri对应的FSDataInputStream文件输入流，读取这个文件
			fin = fs.open(new Path(uri));
			in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
			String line = "";
			while ((line = in.readLine()) != null) {
				String[] splitLine = line.split("\t");
				if (splitLine.length != 3) {
					continue;
				}
				// 实例化分类对象，结构为
				// 分类名称，分类下文档数量，训练集文档总数
				CategoryTotal ct = new CategoryTotal();
				ct.setCategory(splitLine[0]);
				ct.setCategoryFileTotal(new Double(splitLine[1]));
				ct.setTotalFile(new Double(splitLine[2]));
				resultList.add(ct);
			}
		} finally {
			IOUtils.closeStream(in);
		}
		return resultList;
	}

	/**
	 * <h1>根据文件的hdfs路径获取文件内容并且要保存到map中</h1> <li>
	 * map的key为分类名称，value为该分类下的500个词语的朴素贝叶斯结果</li> <li>value的结构为：词语，logp2，p<li>
	 * <li>该文档中某个特征词属于该类别的概率p2<li> <li>（该类别中包含该特征词的文档数+0.8）/（该类别的文档总数+0.8）<li>
	 * <li>P=(p1*文档中每个特征词的p2的累乘积)</li>
	 * 
	 * @param filePath
	 *            文件路径
	 * @return <code>Map<String, List<BayesValue>></code>
	 * @throws IOException
	 */
	public static List<CategoryTotal> getBayesValueFromHdfs(String filePath) throws IOException {
		String uri = filePath;
		Configuration conf = new Configuration();
		conf.set("hadoop.job.ugi", "hadoop-user,hadoop-user");
		// FileSystem是用户操作HDFS的核心类，它获得URI对应的HDFS文件系统
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream fin = null;
		BufferedReader in = null;
		List<CategoryTotal> allList = new ArrayList<CategoryTotal>();
		try {
			// 让FileSystem打开一个uri对应的FSDataInputStream文件输入流，读取这个文件
			fin = fs.open(new Path(uri));
			in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
			String line = "";
			while ((line = in.readLine()) != null) {
				String[] values = line.split("\t");
				if (values.length != 5) {
					continue;
				} else {
					// 构造朴素贝叶斯实例
					CategoryTotal ct = new CategoryTotal();
					ct.setWord(values[0]);
					ct.setCategory(values[1]);
					ct.setTimes(new Double(values[2]));
					ct.setCategoryFileTotal(new Double(values[3]));
					ct.setTotalFile(new Double(values[4]));
					allList.add(ct);
				}
			}
		} finally {
			in.close();
			IOUtils.closeStream(fin);
		}
		// 分组
		groupList(allList);
		return allList;
	}

	/**
	 * 对输入的list按照某一个属性分组
	 * 
	 * @param list
	 * @return
	 */
	public static Map<String, List<CategoryTotal>> groupList(List<CategoryTotal> list) {
		Map<String, List<CategoryTotal>> map = new HashMap<String, List<CategoryTotal>>();
		for (Iterator<CategoryTotal> it = list.iterator(); it.hasNext();) {
			CategoryTotal ct = (CategoryTotal) it.next();
			if (map.containsKey(ct.getWord())) { // 如果已经存在这个数组，就放在这里
				List<CategoryTotal> studentList = map.get(ct.getWord());
				studentList.add(ct);
			} else {
				List<CategoryTotal> studentList = new ArrayList<CategoryTotal>(); // 重新声明一个数组list
				studentList.add(ct);
				map.put(ct.getWord(), studentList);
			}
		}
		return map;
	}
}
