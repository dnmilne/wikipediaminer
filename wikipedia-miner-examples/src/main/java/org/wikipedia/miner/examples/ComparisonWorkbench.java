package org.wikipedia.miner.examples;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;

import org.wikipedia.miner.comparison.ArticleComparer;
import org.wikipedia.miner.comparison.ComparisonDataSet;
import org.wikipedia.miner.comparison.LabelComparer;
import org.wikipedia.miner.db.WDatabase.DatabaseType;
import org.wikipedia.miner.model.Wikipedia;
import org.wikipedia.miner.util.WikipediaConfiguration;

import weka.classifiers.Classifier;
import weka.core.Utils;

public class ComparisonWorkbench {

	
	private Wikipedia _wikipedia ;

	//directory in which files will be stored
	private File _dataDir ;
	
	private File _datasetFile ;
	
	private ComparisonDataSet _dataset ;
	
	//classes for performing comparison
	private ArticleComparer _artComparer ;
	private LabelComparer _labelComparer ;
	
	private File _arffArtCompare, _arffLabelDisambig, _arffLabelCompare ;
	
	private File _modelArtCompare, _modelLabelDisambig, _modelLabelCompare ;
	
	DecimalFormat df = new DecimalFormat("0.0000") ;

	public ComparisonWorkbench(File dataDir, ComparisonDataSet dataset, Wikipedia wikipedia) throws Exception {

		_dataDir = dataDir ;
		_wikipedia = wikipedia ;
		
		_dataset = dataset ;

		_artComparer = new ArticleComparer(_wikipedia) ;
		
		_labelComparer = new LabelComparer(_wikipedia, _artComparer) ;
		
		_arffArtCompare = new File(_dataDir.getPath() + "/art_compare.arff") ;
		_arffLabelDisambig = new File(_dataDir.getPath() + "/lbl_disambig.arff") ;
		_arffLabelCompare = new File(_dataDir.getPath() + "/lbl_compare.arff") ;
		
		_modelArtCompare = new File(_dataDir.getPath() + "/art_compare.model") ;
		_modelLabelDisambig = new File(_dataDir.getPath() + "/lbl_disambig.model") ;
		_modelLabelCompare = new File(_dataDir.getPath() + "/lbl_compare.model") ;
	}
	
	private void createArffFiles(String datasetName) throws IOException, Exception {
		
			_artComparer.train(_dataset);
			_artComparer.buildDefaultClassifier();
			_artComparer.saveTrainingData(_arffArtCompare);
			
			_labelComparer.train(_dataset, datasetName);
			_labelComparer.buildDefaultClassifiers();
			_labelComparer.saveDisambiguationTrainingData(_arffLabelDisambig);
			_labelComparer.saveComparisonTrainingData(_arffLabelCompare);
	}

	private void createClassifiers(String confArtCompare, String confLabelDisambig, String confLabelCompare) throws Exception {
		
		 if (!_arffArtCompare.canRead() || !_arffLabelDisambig.canRead() || !_arffLabelCompare.canRead())
	          throw new Exception("Arff files have not yet been created") ;
		
	      if (confArtCompare == null || confArtCompare.trim().length() == 0) {
	    	  _artComparer.buildDefaultClassifier() ;
	      } else {
	          Classifier classifier = buildClassifierFromOptString(confArtCompare) ;
	          _artComparer.buildClassifier(classifier) ;
	      }
	      _artComparer.saveClassifier(_modelArtCompare) ;
		
	      
	      if (confLabelDisambig == null || confLabelDisambig.trim().length() == 0) {
	    	  _labelComparer.buildDefaultClassifiers() ;
	      } else {
	          Classifier classifierLabelDisambig = buildClassifierFromOptString(confLabelDisambig) ;
	          Classifier classifierLabelCompare = buildClassifierFromOptString(confLabelCompare) ;
	          
	          //TODO: need to use provided classifiers
	          _labelComparer.buildDefaultClassifiers();
	      }
	      
	      _labelComparer.saveDisambiguationClassifier(_modelLabelDisambig);
	      _labelComparer.saveComparisonClassifier(_modelLabelCompare);
	}
	
	 private Classifier buildClassifierFromOptString(String optString) throws Exception {
	      String[] options = Utils.splitOptions(optString) ;
	      String classname = options[0] ;
	      options[0] = "" ;
	      return (Classifier) Utils.forName(Classifier.class, classname, options) ;
	  }

	private void evaluate() throws Exception {
		
		ComparisonDataSet[][] folds = _dataset.getFolds() ;
		
		double totalArtCompare = 0 ;
		double totalLabelDisambig = 0 ;
		double totalLabelCompare = 0 ;
		
		int foldIndex = 0;
		for (ComparisonDataSet[] fold:folds) {
			
			System.out.println("Fold " + foldIndex) ;
			foldIndex++ ;
			
			
			ComparisonDataSet trainingData = fold[0] ;
			ComparisonDataSet testData = fold[1] ;
			
			ArticleComparer artComparer = new ArticleComparer(_wikipedia) ;
			
			artComparer.train(trainingData);
			artComparer.buildDefaultClassifier();
			
			double corrArtCompare = artComparer.test(testData) ;
			System.out.println(" - art comparison: " + df.format(corrArtCompare));
			totalArtCompare += corrArtCompare ;
			
			
			
			LabelComparer lblComparer = new LabelComparer(_wikipedia, artComparer) ;
			lblComparer.train(trainingData, "");
			lblComparer.buildDefaultClassifiers(); 
			
			double accLabelDisambig = lblComparer.testDisambiguationAccuracy(testData) ;
			System.out.println(" - label disambiguation: " + df.format(accLabelDisambig));
			totalLabelDisambig += accLabelDisambig ;
			
			
			double corrLabelCompare = lblComparer.testRelatednessPrediction(testData) ;
			System.out.println(" - label comparison: " + df.format(corrLabelCompare));
			totalLabelCompare += corrLabelCompare ;
		}
		
		System.out.println();
		System.out.println("art comparison (correllation); " + df.format(totalArtCompare/folds.length)) ;
		System.out.println("label disambiguation (accuracy); " + df.format(totalLabelDisambig/folds.length)) ;
		System.out.println("label comparison (correllation); " + df.format(totalLabelCompare/folds.length)) ;
	}
	
	public static void main(String args[]) throws Exception {

		File dataDir = new File(args[0]) ;
		
		File datasetFile = new File(args[1]) ;
		int maxRelatedness = Integer.parseInt(args[2]) ;
		ComparisonDataSet dataset = new ComparisonDataSet(datasetFile, maxRelatedness) ;
		

		WikipediaConfiguration conf = new WikipediaConfiguration(new File(args[3])) ;
		conf.addDatabaseToCache(DatabaseType.label) ;
		conf.addDatabaseToCache(DatabaseType.pageLinksInNoSentences) ;

		Wikipedia wikipedia = new Wikipedia(conf, false) ;

		ComparisonWorkbench trainer = new ComparisonWorkbench(dataDir, dataset, wikipedia) ;
		BufferedReader input = new BufferedReader(new InputStreamReader(System.in)) ;

		while (true) {
			System.out.println("What would you like to do?") ;
			System.out.println(" - [1] create arff files.") ;
			System.out.println(" - [2] create classifiers.") ;
			System.out.println(" - [3] evaluate classifiers.") ;
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
				System.out.println("Dataset name:") ;
				String datasetName = input.readLine() ;

				trainer.createArffFiles(datasetName) ;
				break ;
			case 2:
				System.out.println("Article comparison classifer config (or ENTER to use default):") ;
				String confArtCompare = input.readLine() ;

				System.out.println("Label disambiguation classifer config (or ENTER to use default):") ;
				String confLabelDisambig = input.readLine() ;
				
				System.out.println("Label comparison classifer config (or ENTER to use default):") ;
				String confLabelDetect = input.readLine() ;

				trainer.createClassifiers(confArtCompare, confLabelDisambig, confLabelDetect) ;
				break ;
			case 3:
				trainer.evaluate() ;
				break ;
			default:
				System.out.println("Invalid Input") ;
			}
		}
	}
	
}
