package org.wikipedia.miner.util;

import java.io.File;


import org.wikipedia.miner.db.*;

public class EnvironmentBuilder {

	public static void main(String args[]) throws Exception {
		
		if (args.length != 1) {
			System.out.println("Please specify path to wikipedia configuration file") ;
			System.exit(1) ;
		}
		
		File confFile = new File(args[0])  ;
		if (!confFile.canRead()) {
			System.out.println("'" + args[0] + "' cannot be read") ;
			System.exit(1) ;
		}
		
		WikipediaConfiguration conf = new WikipediaConfiguration(confFile) ;
		
		if (conf.getDataDirectory() == null || !conf.getDataDirectory().isDirectory()) {
                        System.out.println(conf.getDataDirectory());
			System.out.println("'" + args[0] + "' does not specify a valid data directory") ;
			System.exit(1) ;
		}
	
		WEnvironment.buildEnvironment(conf, conf.getDataDirectory(), false) ;
	}
	
}
