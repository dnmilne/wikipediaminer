package steps2;

import java.io.IOException;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.wikipedia.miner.extract.DumpExtractor;
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.model.struct.PageKey;
import org.wikipedia.miner.extract.model.struct.PageSummary;


public class SubsequentPageSummaryStep extends PageSummaryStep {

	private int iteration ;

	public SubsequentPageSummaryStep(Path baseWorkingDir, int iteration) throws IOException {
		super(baseWorkingDir);

		this.iteration = iteration ;
	}

	@Override
	public int run(String[] args) throws UncompletedStepException, IOException {

		if (isFinished())
			return 0 ;
		else
			reset() ;

		JobConf conf = new JobConf(InitialPageSummaryStep.class);
		DumpExtractor.configureJob(conf, args) ;

		conf.setJobName("WM: page summary step " + iteration);

		AvroJob.setInputSchema(conf, Pair.getPairSchema(PageKey.getClassSchema(),PageDetail.getClassSchema()));
		AvroJob.setOutputSchema(conf, Pair.getPairSchema(PageKey.getClassSchema(),PageDetail.getClassSchema()));


		AvroJob.setMapperClass(conf, Map.class);
		AvroJob.setReducerClass(conf, InitialPageSummaryStep.Reduce.class);


		DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.OUTPUT_SITEINFO).toUri(), conf);
		DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_LANG_FILE)).toUri(), conf);

		FileInputFormat.setInputPaths(conf, getBaseWorkingDir() + "/" + "page_" + (iteration-1));
		FileOutputFormat.setOutputPath(conf, getWorkingDir());

		RunningJob runningJob = JobClient.runJob(conf);

		finish(runningJob) ;

		if (runningJob.getJobState() == JobStatus.SUCCEEDED)
			return 0 ;
		
		throw new UncompletedStepException() ;
	}

	@Override
	public String getWorkingDirName() {


		return "page_" + iteration;
	}

	protected static class Map extends AvroMapper<Pair<PageKey, PageDetail>, Pair<PageKey, PageDetail>> {

		@Override
		public void map(Pair<PageKey, PageDetail> pair,
				AvroCollector<Pair<PageKey, PageDetail>> collector,
				Reporter reporter) throws IOException {
			
			PageKey pageKey = pair.key() ;
			PageDetail page = pair.value() ;
			
			
			if (page.getRedirectsTo() != null) {
				
				CharSequence targetTitle = page.getRedirectsTo().getTitle() ;
				
				//forward any redirects to redirects
				
				for (PageSummary redirect:page.getRedirects()) {
					
					if (redirect.getForwarded())
						continue ;
					
					//forward this redirect to the target of this page (so we are following down the redirect chain)
					PageKey targetKey = new PageKey(page.getNamespace(), targetTitle) ;
					PageDetail target = InitialPageSummaryStep.buildEmptyPageDetail() ;
					target.getRedirects().add(PageSummary.newBuilder(redirect).build()) ;
					
					collector.collect(new Pair<PageKey,PageDetail>(targetKey, target));
					
					//and record that it has been forwarded
					redirect.setForwarded(true);
				}
				
				for (PageSummary linkIn:page.getLinksIn()) {
					
					if (linkIn.getForwarded())
						continue ;
					
					//forward this link to the target of this page (so we are following down the redirect chain)
					PageKey targetKey = new PageKey(page.getNamespace(), targetTitle) ;
					PageDetail target = InitialPageSummaryStep.buildEmptyPageDetail() ;
					target.getLinksIn().add(PageSummary.newBuilder(linkIn).build()) ;
					
					collector.collect(new Pair<PageKey,PageDetail>(targetKey, target));
					
					linkIn.setForwarded(true);
				}
				
				for (PageSummary childCategory:page.getChildCategories()) {
					
					if (childCategory.getForwarded())
						continue ;
					
					PageKey targetKey = new PageKey(page.getNamespace(), targetTitle) ;
					PageDetail target = InitialPageSummaryStep.buildEmptyPageDetail() ;
					target.getChildCategories().add(PageSummary.newBuilder(childCategory).build()) ;
					
					collector.collect(new Pair<PageKey,PageDetail>(targetKey, target));
				
					childCategory.setForwarded(true);
				}
				
				for (PageSummary childArticle:page.getChildCategories()) {
					
					if (childArticle.getForwarded())
						continue ;
					
					PageKey targetKey = new PageKey(page.getNamespace(), targetTitle) ;
					PageDetail target = InitialPageSummaryStep.buildEmptyPageDetail() ;
					target.getChildArticles().add(PageSummary.newBuilder(childArticle).build()) ;
					
					collector.collect(new Pair<PageKey,PageDetail>(targetKey, target));
				
					childArticle.setForwarded(true);
				}
				
				//redirects should not get any links out or parent relations
				
			} else {
				
				for (PageSummary redirect:page.getRedirects()) {
					
					if (redirect.getForwarded())
						continue ;
					
					//backtrack, so the redirect knows what the resolved target is
					PageKey redirectKey = new PageKey(redirect.getNamespace(), redirect.getTitle()) ;
					PageDetail redirectDetail = InitialPageSummaryStep.buildEmptyPageDetail() ;
					redirectDetail.setRedirectsTo(new PageSummary(page.getId(), page.getTitle(), page.getNamespace(), false));
					
					collector.collect(new Pair<PageKey,PageDetail>(redirectKey, redirectDetail));
					
					//and record that it has been forwarded
					redirect.setForwarded(true);
				}
				
				for (PageSummary linkIn:page.getLinksIn()) {
					
					if (linkIn.getForwarded())
						continue ;
					
					//backtrack, so the source of this link knows what the resolved target is
					PageKey sourceKey = new PageKey(linkIn.getNamespace(), linkIn.getTitle()) ;
					PageDetail sourceDetail = InitialPageSummaryStep.buildEmptyPageDetail() ;
					sourceDetail.getLinksOut().add(new PageSummary(page.getId(), page.getTitle(), page.getNamespace(), false));
					
					collector.collect(new Pair<PageKey,PageDetail>(sourceKey, sourceDetail));
					
					//and record that it has been forwarded
					linkIn.setForwarded(true);
				}
				
				for (PageSummary linkOut:page.getLinksOut()) {
					
					//immediately set these as forwarded, because we only get them if they have been forwarded and backtracked already					
					linkOut.setForwarded(true);
				}
				
				for (PageSummary childCategory:page.getChildCategories()) {
					
					if (childCategory.getForwarded())
						continue ;
					
					//backtrack, so the child knows what the resolved parent is
					PageKey childKey = new PageKey(childCategory.getNamespace(), childCategory.getTitle()) ;
					PageDetail childDetail = InitialPageSummaryStep.buildEmptyPageDetail() ;
					childDetail.getParentCategories().add(new PageSummary(page.getId(), page.getTitle(), page.getNamespace(), false));
					
					collector.collect(new Pair<PageKey,PageDetail>(childKey, childDetail));
					
					//and record that it has been forwarded
					childCategory.setForwarded(true);
				}
				
				for (PageSummary childArticle:page.getChildArticles()) {
					
					if (childArticle.getForwarded())
						continue ;
					
					//backtrack, so the child knows what the resolved parent is
					PageKey childKey = new PageKey(childArticle.getNamespace(), childArticle.getTitle()) ;
					PageDetail childDetail = InitialPageSummaryStep.buildEmptyPageDetail() ;
					childDetail.getParentCategories().add(new PageSummary(page.getId(), page.getTitle(), page.getNamespace(), false));
					
					collector.collect(new Pair<PageKey,PageDetail>(childKey, childDetail));
					
					//and record that it has been forwarded
					childArticle.setForwarded(true);
				}
				
				for (PageSummary parentCategory:page.getParentCategories()) {
					//immediately set these as forwarded, because we only get them if they have been forwarded and backtracked already					
					parentCategory.setForwarded(true);
				}
		
			}
			
			collector.collect(new Pair<PageKey,PageDetail>(pageKey, page));
		}
	}
}
