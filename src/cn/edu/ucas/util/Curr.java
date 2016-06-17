package cn.edu.ucas.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Curr {

	public static void main(String[] args) throws IOException {
		String uri = args[0];
		double total = 0d;
		double curr = 0d;
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
				if (splitLine.length != 4) {
					continue;
				}
				// if (splitLine[0].equals("IT")) {
				total++;
				if (splitLine[0].equals(splitLine[2])) {
					curr++;
				}
				// }
			}
			System.out.println("准确率为:" + (curr / total) * 100 + "%");
		} finally {
			IOUtils.closeStream(in);
		}
		return;
	}
}
