package steps2;

import java.io.IOException;
import java.util.ArrayList;

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
import org.wikipedia.miner.extract.model.struct.LabelCount;
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.model.struct.PageKey;
import org.wikipedia.miner.extract.model.struct.PageSummary;


public class SubsequentPageSummaryStep extends PageSummaryStep {

	/*
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
				
				//this is a redirect, so it has to be treated very differently from other page types
				//is kind of a conduit that relations need to be forwarded through
				
				
				CharSequence targetTitle = page.getRedirectsTo().getTitle() ;
				
				PageKey targetKey = new PageKey(page.getNamespace(), targetTitle) ;
				PageDetail target = InitialPageSummaryStep.buildEmptyPageDetail() ;
				
				
				
				//if this target is resolved (we know its id), backtrack it to any redirects that point to this page
				//so that they will also know what their eventual target is
				if (page.getRedirectsTo().getId() > 0 && !page.getRedirectsTo().getForwarded()) {
					
					for (PageSummary redirect:page.getRedirects()) {
						//backtrack this redirect to the target of this page (so we are following down the redirect chain)
						PageKey redirectKey = new PageKey(redirect.getNamespace(), redirect.getTitle()) ;
						PageDetail redirectDetail = InitialPageSummaryStep.buildEmptyPageDetail() ;
						redirectDetail.setRedirectsTo(PageSummary.newBuilder(page.getRedirectsTo()).build()) ;
						
						collector.collect(new Pair<PageKey,PageDetail>(redirectKey, redirectDetail));
					}
					
					//and record that it has been backtracked
					
					page.getRedirectsTo().setForwarded(true) ;
				}
				
				
				
				
				//if this redirect receives any redirects, forward them on to the target
				
				for (PageSummary redirect:page.getRedirects()) {
					
					if (redirect.getForwarded())
						continue ;
					
					//forward this redirect to the target of this page (so we are following down the redirect chain)
					target.getRedirects().add(PageSummary.newBuilder(redirect).build()) ;
					
					
					//and record that it has been forwarded
					redirect.setForwarded(true);
				}
				
				for (PageSummary linkIn:page.getLinksIn()) {
					
					if (linkIn.getForwarded())
						continue ;
					
					//forward this link to the target of this page (so we are following down the redirect chain)
					target.getLinksIn().add(PageSummary.newBuilder(linkIn).build()) ;
					
					linkIn.setForwarded(true);
				}
				
				for (PageSummary childCategory:page.getChildCategories()) {
					
					if (childCategory.getForwarded())
						continue ;
					
					target.getChildCategories().add(PageSummary.newBuilder(childCategory).build()) ;
					
					childCategory.setForwarded(true);
				}
				
				for (PageSummary childArticle:page.getChildCategories()) {
					
					if (childArticle.getForwarded())
						continue ;
					
					target.getChildArticles().add(PageSummary.newBuilder(childArticle).build()) ;
					
					childArticle.setForwarded(true);
				}
				
				//redirects should not get any links out or parent relations, so do nothing with those
				
				//immediately pass on any label counts to target 
				for (LabelCount labelCount:page.getLabelCounts()) {
					target.getLabelCounts().add(labelCount) ;
				}
				//and remove them from here (otherwise they will get counted multiple times)
				page.setLabelCounts(new ArrayList<LabelCount>()) ;
				
				//emit the details of the target that we have built up
				collector.collect(new Pair<PageKey,PageDetail>(targetKey, target));
				
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
			
			//emit the page, so we can pick it up again in the reducer
			collector.collect(new Pair<PageKey,PageDetail>(pageKey, page));
		}
	}
	*/
}
