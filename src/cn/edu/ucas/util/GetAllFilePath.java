package cn.edu.ucas.util;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;

/**
 * 
 * @author cqyyjdw
 * @date 2014年4月13日
 * @version 0.0.1
 * @function 根据用户的输入路径得到该路径下面的所有文件，需要遍历目录
 */
public class GetAllFilePath {
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}

	public GetAllFilePath() {
	}

	/**
	 * 
	 * @param inputPath
	 *            输入路径----根目录
	 * @return 根目录下面的所有目录
	 */
	public static String[] getDic(String inputPath, Configuration config) {
		List<String> dics = new ArrayList<String>();
		try {
			Configuration conf = config;
			FileSystem fs = FileSystem.get(URI.create(inputPath), conf);
			if (!fs.getFileStatus(new Path(inputPath)).isDir()) {
				throw new Exception(inputPath + " is a file, pls input a dic path");
			}
			FileStatus[] fileStatus = fs.listStatus(new Path(inputPath));
			for (int i = 0; i < fileStatus.length; i++) {
				// 如果是目录就添加到list集合中去
				if (fileStatus[i].isDir()) {
					dics.add(fileStatus[i].getPath().toString());
					String[] tmp = getDic(fileStatus[i].getPath().toString(), conf);
					for (int j = 0; j < tmp.length; j++) {
						dics.add(tmp[j]);
					}
				} else {
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		String[] dirPaths = new String[dics.size()];
		dics.toArray(dirPaths);
		return dirPaths;
	}
}