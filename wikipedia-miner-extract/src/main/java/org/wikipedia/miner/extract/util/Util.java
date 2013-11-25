package org.wikipedia.miner.extract.util;


import java.io.File;

import org.apache.hadoop.fs.Path;


public class Util {

	public static String normaliseTitle(String title) {

		StringBuffer s = new StringBuffer() ;

		s.append(Character.toUpperCase(title.charAt(0))) ;
		s.append(title.substring(1).replace('_', ' ')) ;

		return s.toString().trim() ;
	}

	public static long getFileSize(Path path) {

		File file = new File(path.toString()) ;

		return file.length() ;
	}
}
