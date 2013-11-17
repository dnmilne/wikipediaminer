package org.wikipedia.miner.util;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLStreamException;


import org.wikipedia.miner.db.*;
import org.wikipedia.miner.util.text.PorterStemmer;

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
			System.out.println("'" + args[0] + "' does not specify a valid data directory") ;
			System.exit(1) ;
		}
	
		WEnvironment.buildEnvironment(conf, conf.getDataDirectory(), false) ;
	}
	
}
