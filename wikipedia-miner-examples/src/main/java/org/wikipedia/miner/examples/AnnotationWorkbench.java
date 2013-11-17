package org.wikipedia.miner.examples;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.wikipedia.miner.annotation.ArticleCleaner.SnippetLength;
import org.wikipedia.miner.annotation.Disambiguator;
import org.wikipedia.miner.annotation.TopicDetector;
import org.wikipedia.miner.annotation.weighting.LinkDetector;
import org.wikipedia.miner.db.WDatabase.DatabaseType;
import org.wikipedia.miner.model.Wikipedia;
import org.wikipedia.miner.util.ArticleSet;
import org.wikipedia.miner.util.ArticleSetBuilder;
import org.wikipedia.miner.util.Result;
import org.wikipedia.miner.util.WikipediaConfiguration;

import weka.classifiers.Classifier;
import weka.core.Utils;

public class AnnotationWorkbench {

	private Wikipedia _wikipedia ;

	//directory in which files will be stored
	private File _dataDir ;

	//classes for performing annotation
	private Disambiguator _disambiguator ;
	private TopicDetector _topicDetector ;
	private LinkDetector _linkDetector ;

	//article set files
	private File _artsTrain, _artsTestDisambig, _artsTestDetect ;

	//feature data files
	private File _arffDisambig, _arffDetect ;

	//model files
	private File _modelDisambig, _modelDetect ;


	public AnnotationWorkbench(File dataDir, Wikipedia wikipedia) throws Exception {

		_dataDir = dataDir ;
		_wikipedia = wikipedia ;

		_disambiguator = new Disambiguator(_wikipedia) ;
		_topicDetector = new TopicDetector(_wikipedia, _disambiguator) ;
		_linkDetector = new LinkDetector(_wikipedia) ;

		_artsTrain = new File(_dataDir.getPath() + "/articlesTrain.csv") ;
		_artsTestDisambig = new File(_dataDir.getPath() + "/articlesTestDisambig.csv") ;
		_artsTestDetect = new File(_dataDir.getPath() + "/articlesTestDetect.csv") ;

		_arffDisambig = new File(_dataDir.getPath() + "/disambig.arff") ;
		_arffDetect = new File(_dataDir.getPath() + "/detect.arff") ;

		_modelDisambig = new File(_dataDir.getPath() + "/disambig.model") ;
		_modelDetect = new File(_dataDir.getPath() + "/detect.model") ;
	}

	private void gatherArticleSets() throws IOException{
		 int[] sizes = {200,100,100} ;

	      ArticleSet[] articleSets = new ArticleSetBuilder()
	          .setMinOutLinks(15)
	          .setMinInLinks(20)
	          .setMaxListProportion(0.1)
	          .setMinWordCount(200)
	          .setMaxWordCount(2000)
	          .buildExclusiveSets(sizes, _wikipedia) ;
		
	      articleSets[0].save(_artsTrain) ;
	      articleSets[1].save(_artsTestDisambig) ;
	      articleSets[2].save(_artsTestDetect) ;
	}

	private void createArffFiles(String datasetName) throws IOException, Exception {
		if (!_artsTrain.canRead()) 
	          throw new Exception("Article sets have not yet been created") ;
		
	      ArticleSet trainingSet = new ArticleSet(_artsTrain, _wikipedia) ;
		
	      _disambiguator.train(trainingSet, SnippetLength.full, datasetName + "_disambiguation", null) ;
	      _disambiguator.saveTrainingData(_arffDisambig) ;
	      _disambiguator.buildDefaultClassifier();
	      
		
	      _linkDetector.train(trainingSet, SnippetLength.full, datasetName + "_detection", _topicDetector, null) ;
	      _linkDetector.saveTrainingData(_arffDetect) ;
	}

	private void createClassifiers(String configDisambig, String configDetect) throws Exception {
		 if (!_arffDisambig.canRead() || !_arffDetect.canRead())
	          throw new Exception("Arff files have not yet been created") ;
		
	      _disambiguator.loadTrainingData(_arffDisambig) ;
	      if (configDisambig == null || configDisambig.trim().length() == 0) {
	          _disambiguator.buildDefaultClassifier() ;
	      } else {
	          Classifier classifier = buildClassifierFromOptString(configDisambig) ;
	          _disambiguator.buildClassifier(classifier) ;
	      }
	      _disambiguator.saveClassifier(_modelDisambig) ;
		
	      _linkDetector.loadTrainingData(_arffDetect) ;
	      if (configDetect == null || configDisambig.trim().length() == 0) {
	          _linkDetector.buildDefaultClassifier() ;
	      } else {
	          Classifier classifier = buildClassifierFromOptString(configDisambig) ;
	          _linkDetector.buildClassifier(classifier) ;
	      }
	      _linkDetector.saveClassifier(_modelDetect) ;
	}
	
	 private Classifier buildClassifierFromOptString(String optString) throws Exception {
	      String[] options = Utils.splitOptions(optString) ;
	      String classname = options[0] ;
	      options[0] = "" ;
	      return (Classifier) Utils.forName(Classifier.class, classname, options) ;
	  }

	private void evaluate() throws Exception {
		if (!_modelDisambig.canRead() || !_modelDetect.canRead()) 
	          throw(new Exception("Classifier models have not yet been created")) ;
		
	      if (!_artsTestDisambig.canRead() || !_artsTestDetect.canRead()) 
	          throw(new Exception("Article sets have not yet been created")) ;
		
	      ArticleSet disambigSet = new ArticleSet(_artsTestDisambig, _wikipedia) ;
	      _disambiguator.loadClassifier(_modelDisambig) ;
	      Result<Integer> disambigResults = _disambiguator.test(disambigSet, _wikipedia, SnippetLength.full, null) ;
		
	      ArticleSet detectSet = new ArticleSet(_artsTestDetect, _wikipedia) ;
	      _linkDetector.loadClassifier(_modelDetect) ;
	      Result<Integer> detectResults = _linkDetector.test(detectSet, SnippetLength.full, _topicDetector, null) ;
		
	      System.out.println();
	      System.out.println("Disambig results: " + disambigResults) ;
	      System.out.println("Detect results: " + detectResults) ;
	}

	public static void main(String args[]) throws Exception {

		File dataDir = new File(args[0]) ;

		WikipediaConfiguration conf = new WikipediaConfiguration(new File(args[1])) ;
		conf.addDatabaseToCache(DatabaseType.label) ;
		conf.addDatabaseToCache(DatabaseType.pageLinksInNoSentences) ;

		Wikipedia wikipedia = new Wikipedia(conf, false) ;

		AnnotationWorkbench trainer = new AnnotationWorkbench(dataDir, wikipedia) ;
		BufferedReader input = new BufferedReader(new InputStreamReader(System.in)) ;

		while (true) {
			System.out.println("What would you like to do?") ;
			System.out.println(" - [1] create article sets.") ;
			System.out.println(" - [2] create arff files.") ;
			System.out.println(" - [3] create classifiers.") ;
			System.out.println(" - [4] evaluate classifiers.") ;
			System.out.println(" - or ENTER to quit.") ;

			String line = input.readLine() ;

			if (line.trim().length() == 0)
				break ;

			Integer choice = 0 ;
			try {
				choice = Integer.parseInt(line) ;
			} catch (Exception e) {
				System.out.println("Invalid Input") ;
				continue ;
			}

			switch(choice) {
			case 1:
				trainer.gatherArticleSets() ;
				break ;
			case 2:
				System.out.println("Dataset name:") ;
				String datasetName = input.readLine() ;

				trainer.createArffFiles(datasetName) ;
				break ;
			case 3:
				System.out.println("Disambiguation classifer config (or ENTER to use default):") ;
				String configDisambig = input.readLine() ;

				System.out.println("Detection classifer config (or ENTER to use default):") ;
				String configDetect = input.readLine() ;

				trainer.createClassifiers(configDisambig, configDetect) ;
				break ;
			case 4:
				trainer.evaluate() ;
				break ;
			default:
				System.out.println("Invalid Input") ;
			}
		}
	}
	
	
	
}