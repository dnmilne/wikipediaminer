package org.wikipedia.miner.extract.steps.pageSummary;

import java.io.IOException;
import java.util.HashMap;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.Reporter;
import org.wikipedia.miner.extract.model.struct.LabelSummary;
import org.wikipedia.miner.extract.model.struct.LinkSummary;
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.model.struct.PageKey;
import org.wikipedia.miner.extract.model.struct.PageSummary;

public class SubsequentMapper extends AvroMapper<Pair<PageKey, PageDetail>, Pair<PageKey, PageDetail>> {

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
			
			PageKey targetKey = new PageKey(pageKey.getNamespace(), targetTitle) ;
			PageDetail target = PageSummaryStep.buildEmptyPageDetail() ;
			
			
			
			//if this target is resolved (we know its id), backtrack it to any redirects that point to this page
			//so that they will also know what their eventual target is
			if (page.getRedirectsTo().getId() > 0 && !page.getRedirectsTo().getForwarded()) {
				
				for (PageSummary redirect:page.getRedirects()) {
					//backtrack this redirect to the target of this page (so we are following down the redirect chain)
					PageKey redirectKey = new PageKey(redirect.getNamespace(), redirect.getTitle()) ;
					PageDetail redirectDetail = PageSummaryStep.buildEmptyPageDetail() ;
					redirectDetail.setRedirectsTo(PageSummaryStep.clone(page.getRedirectsTo())) ;
					
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
				target.getRedirects().add(PageSummaryStep.clone(redirect)) ;
				
				
				//and record that it has been forwarded
				redirect.setForwarded(true);
			}
			
			for (LinkSummary linkIn:page.getLinksIn()) {
				
				if (linkIn.getForwarded())
					continue ;
				
				//forward this link to the target of this page (so we are following down the redirect chain)
				target.getLinksIn().add(PageSummaryStep.clone(linkIn)) ;
				
				linkIn.setForwarded(true);
			}
			
			for (PageSummary childCategory:page.getChildCategories()) {
				
				if (childCategory.getForwarded())
					continue ;
				
				target.getChildCategories().add(PageSummaryStep.clone(childCategory)) ;
				
				childCategory.setForwarded(true);
			}
			
			for (PageSummary childArticle:page.getChildCategories()) {
				
				if (childArticle.getForwarded())
					continue ;
				
				target.getChildArticles().add(PageSummaryStep.clone(childArticle)) ;
				
				childArticle.setForwarded(true);
			}
			
			//redirects should not get any links out or parent relations, so do nothing with those
			
			//immediately pass on any label counts to target
			target.setLabels(page.getLabels());
			
			//and remove them from here (otherwise they will get counted multiple times)
			page.setLabels(new HashMap<CharSequence,LabelSummary>()) ;
			
			//emit the details of the target that we have built up
			collector.collect(new Pair<PageKey,PageDetail>(targetKey, target));
			
		} else {
			
			for (PageSummary redirect:page.getRedirects()) {
				
				if (redirect.getForwarded())
					continue ;
				
				//backtrack, so the redirect knows what the resolved target is
				PageKey redirectKey = new PageKey(redirect.getNamespace(), redirect.getTitle()) ;
				PageDetail redirectDetail = PageSummaryStep.buildEmptyPageDetail() ;
				redirectDetail.setRedirectsTo(new PageSummary(page.getId(), pageKey.getTitle(), pageKey.getNamespace(), false));
				
				collector.collect(new Pair<PageKey,PageDetail>(redirectKey, redirectDetail));
				
				//and record that it has been forwarded
				redirect.setForwarded(true);
			}
			
			for (LinkSummary linkIn:page.getLinksIn()) {
				
				if (linkIn.getForwarded())
					continue ;
				
				//backtrack, so the source of this link knows what the resolved target is
				PageKey sourceKey = new PageKey(linkIn.getNamespace(), linkIn.getTitle()) ;
				PageDetail sourceDetail = PageSummaryStep.buildEmptyPageDetail() ;
				sourceDetail.getLinksOut().add(new LinkSummary(page.getId(), pageKey.getTitle(), pageKey.getNamespace(), false, linkIn.getSentenceIndexes()));
				
				collector.collect(new Pair<PageKey,PageDetail>(sourceKey, sourceDetail));
				
				//and record that it has been forwarded
				linkIn.setForwarded(true);
			}
			
			for (LinkSummary linkOut:page.getLinksOut()) {
				
				//immediately set these as forwarded, because we only get them if they have been forwarded and backtracked already					
				linkOut.setForwarded(true);
			}
			
			for (PageSummary childCategory:page.getChildCategories()) {
				
				if (childCategory.getForwarded())
					continue ;
				
				//backtrack, so the child knows what the resolved parent is
				PageKey childKey = new PageKey(childCategory.getNamespace(), childCategory.getTitle()) ;
				PageDetail childDetail = PageSummaryStep.buildEmptyPageDetail() ;
				childDetail.getParentCategories().add(new PageSummary(page.getId(), pageKey.getTitle(), pageKey.getNamespace(), false));
				
				collector.collect(new Pair<PageKey,PageDetail>(childKey, childDetail));
				
				//and record that it has been forwarded
				childCategory.setForwarded(true);
			}
			
			for (PageSummary childArticle:page.getChildArticles()) {
				
				if (childArticle.getForwarded())
					continue ;
				
				//backtrack, so the child knows what the resolved parent is
				PageKey childKey = new PageKey(childArticle.getNamespace(), childArticle.getTitle()) ;
				PageDetail childDetail = PageSummaryStep.buildEmptyPageDetail() ;
				childDetail.getParentCategories().add(new PageSummary(page.getId(), pageKey.getTitle(), pageKey.getNamespace(), false));
				
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