function doEventBindings() {
	$("form").submit(function() {
		var ok = true;
		
		if ($('#query').val() == "") {
			$('#query').addClass("ui-state-error") ;
			ok = false ;
		}
		
		return ok ;
    });
	
	$('#cmdSearch').button() ;
}

function doTooltipBindings() {
	
	
	$('#sensesHelp').qtip({
	      content: "It looks like your query is ambiguous, because there are multiple articles that it could refer to. So, which one do you want?",
    }) ;
	
	$('#labelHelp').qtip({
	      content: "Terms that are used to refer to this article. These are usually synonyms or alternative spellings.",
    }) ;
	
	$('#translationHelp').qtip({
	      content: "Links to articles which describe the same concept in another language.",
    }) ;
	
	$('#categoryHelp').qtip({
	      content: "Categories to which this article belongs. These represent broader topics or ways of organizing this one.",
    }) ;
    
    $('#linksOutHelp').qtip({
	      content: "<p>Pages that this article links to. Some of these represent related topics, others are fairly random.</p> <p>The toolkit provides relatedness measures&mdash;indicated here by the brightness of each link&mdash;to help separate them.</p>",
    }) ;
    
    $('#linksInHelp').qtip({
	      content: "<p>Pages that link to this article. Some of these represent related topics, others are fairly random.</p> <p>The toolkit provides relatedness measures&mdash;indicated here by the brightness of each link&mdash;to help separate them.</p>",
    }) ;
	
	$('#parentCatHelp').qtip({
	      content: "Categories to which this category belongs. These represent broader topics or ways of organizing this one.",
    }) ;
	
	$('#childCatHelp').qtip({
	      content: "Categories which belong to this category. These represent narrower topics.",
    }) ;
	
	$('#childArtHelp').qtip({
	      content: "Articles which belong to this category. These represent narrower topics.",
    }) ;
	
}



$(document).ready(function() {
	
	wm_setHost("../../")
	
	doEventBindings() ;
	doTooltipBindings() ;
	
	

	checkProgress() ;
}) ;

function checkProgress() {
	
	$.getJSON(
		"../../services/getProgress",
		{responseFormat:'json'},
		function(data) {
			var progress = data.progress ;

			if (progress >= 1) {
				ready() ;
			} else {
		
				$('#progress').progressbar(
					{value: Math.floor(progress*100)}
				) ;
		
				setTimeout(
					checkProgress,
					500
				) ;
			}
		}
	) ;	
}


function ready() {
	
	$("#initializing").hide() ;
	$("#ready").show() ;
	
	var query = urlParams["query"];
	var artId = urlParams["artId"];
	var catId = urlParams["catId"];
	
	
	if (artId != undefined || catId != undefined || query != undefined) {
		$('#searchbox').removeClass('majorSearch') ;
		$('#searchbox').addClass('minorSearch') ;
	}
		
	if (artId != undefined) {
		
		$('#instructions').hide() ;
		$('#loading').show() ;
		
		
		$.get(
			"../../services/exploreArticle", 
			{
				id: artId,
				definition: true,
				definitionLength:'LONG',
				linkFormat: 'WIKI_ID',
				labels:true,
				translations:true,
				images:true,
				maxImageWidth:'300',
				maxImageHeight:'300',
				parentCategories:true,
				inLinks:true,
				inLinkMax: 100,
				outLinks:true,
				outLinkMax: 100,
				linkRelatedness:true,
				responseFormat:'json'
			},
			function(data){
				processArticleResponse(data) ;
			}
		);
		
		return ;
	}
	
	if (catId != undefined) {
		
		$('#instructions').hide() ;
		$('#loading').show() ;
		
		$.get(
			"../../services/exploreCategory", 
			{
				id: catId,
				parentCategories:true,
				childCategories: true,
				childArticles: true,
				childArticleLimit: 100,
				responseFormat:'json'
			},
			function(data){
				processCategoryResponse(data) ;
			}
		);
		
		return ;
	}
	
	
	if (query != undefined) {
		
		$('#query').val(query) ;
		
		$('#instructions').hide() ;
		$('#loading').show() ;
		
		$.get(
			"../../services/search", 
			{
				query: query,
				responseFormat:'json'
			},
			function(data){
				processSearchResponse(data) ;
			}
		);
	}
	
}


function processSearchResponse(data) {
	
	$('#loading').hide() ;
	
	
	var label = data.labels[0] ;
	var senses = label.senses ;	
	
	if (senses.length > 1) {
		$('#senseDetails').show() ;
		
		$.each(senses, function() {
			var sense = this ;
			
			var pp = Number(sense.priorProbability) ;
			
			var senseBox = $("<div class='senseBox' id='senseBox_" + sense.id + "'></div>") ;
			
			var bar = $("<div class='bar'></div>") ;
			bar.append("<div style='width:" + Math.floor(pp*50) + "px'></div>") ;
			
			bar.qtip({
				content: "<em>" + Math.floor(pp*100) + "%</em> of <i>" + urlParams["query"] + "</i> occurrences refer to this.",
				style: { name: 'wmstyle' }
			}) ;
			
			senseBox.append(bar) ;
			
			senseBox.append("<a class='title' href='?artId=" + sense.id + "'>" + sense.title + "</a>") ;
			
			
			senseBox.append("<p class='definition'><img src='../images/loading.gif'></img></p>") ;
			
			$.get(
				"../../services/exploreArticle", 
				{
					id: sense.id, 
					definition:true,
					linkFormat:'PLAIN',
					responseFormat:'JSON'
				},
				function(data){
					
					var id = data.request.id ;
					var definition = data.definition ;
					
					var senseBox = $('#senseBox_' + id)
					
					if (definition == undefined || definition == "")
						senseBox.find('.definition').html("No definition could be found") ;
					else
						senseBox.find('.definition').html(definition) ;
				}
			);
			
			$('#senses').append(senseBox) ;
		}) ;

	} else if (senses.length == 1){
		
		$('#loading').show() ;
		
		$.get(
			"../../services/exploreArticle", 
			{
				id: senses[0].id,
				definition: true,
				definitionLength:'LONG',
				linkFormat: 'WIKI_ID',
				labels:true,
				translations:true,
				images:true,
				maxImageWidth:'300',
				maxImageHeight:'300',
				parentCategories:true,
				inLinks:true,
				inLinkMax: 100,
				outLinks:true,
				outLinkMax: 100,
				linkRelatedness:true,
				responseFormat:'JSON'
			},
			function(data){
				processArticleResponse(data) ;
			}
		);
		
	} else {
		
		$('#unkownTerm').html(urlParams["query"]) ;
		
		$('#senseDetails').hide() ;
		$('#error').show() ;
		
		
	}
}


function processArticleResponse(data) {
	
	$('#loading').hide() ;
	$('#articleDetails').show() ;
	

	
	$('#artTitle').html(data.title) ;
	
	var origDefinition = data.definition ;
	
	if (origDefinition == undefined || origDefinition.length == 0)
		origDefinition = "No definition avaialble" ;
	
	var newDefinition = "" ;
	
	var lastIndex = 0 ;
		
	var pattern=/\[\[(\d+)\|(.*?)\]\]/g ;
	var result;
	while ((result = pattern.exec(origDefinition)) != null) {			
		newDefinition = newDefinition + origDefinition.substring(lastIndex, result.index) ;
		
		var id = result[1] ;
		var anchor = result[2] ;
		
		newDefinition = newDefinition + "<a href=\"./?artId=" + id + "\" pageId='" + id + "'>" + anchor + "</a>" ;
		
		lastIndex = pattern.lastIndex ;
	}
	newDefinition = newDefinition + origDefinition.substring(lastIndex) ;

	$('#artDefinition').html(newDefinition) ;
	
	wm_addDefinitionTooltipsToAllLinks($('#artDefinition')) ;
	
	
	var sortedLabels = data.labels.sort(function(a,b) {
		var valA = a.text ;
		var valB = b.text ;
		
		return valA < valB ? -1 : valA == valB? 0 : 1 ;
	}) ;
	
	if (sortedLabels.length > 0) {
		
		var maxWeight, minWeight ;
		$.each(sortedLabels, function() {
			var weight = this.proportion ;
			
			if (minWeight == undefined || weight < minWeight) minWeight = weight ;
			if (maxWeight == undefined || weight > maxWeight) maxWeight = weight ;
		}) ;
		
		$.each(sortedLabels, function() {
			//var xmlLabel = $(this) ;
			var sortedLabel = this ;
			
			var label = $("<li>" + sortedLabel.text + "</li>") ;
			var weight = Number(sortedLabel.proportion) ;
			weight = normalize(weight, minWeight, maxWeight) ;
						
			if (weight > 0.01) {
				label.css('font-size', getFontSize(weight) + "px") ;
				label.css('color', getFontColor(weight)) ;
				
				label.qtip({
					content: "used <em>" + sortedLabel.occurrances + "</em> times",
					style: { name: 'wmstyle' }
				}) ;
				
	
				$('#labels').append(label) ;
			}
		}) ;
	} else {
		$('#noLabels').show() ;
	}
	
	var sortedTranslations = data.translations.sort(function(a,b) {
		var valA = a.lang ;
		var valB = b.lang ;
		
		return valA < valB ? -1 : valA == valB? 0 : 1 ;
	}) ;
	
	if (sortedTranslations.length > 0) {
		$.each(sortedTranslations, function() {			
			var translation = $("<li><em>" + this.lang + "</em>: " + this.text + "</li>") ;
			$('#translations').append(translation) ;
		}) ;
	} else {
		$('#noTranslations').show() ;
	}
	
	var sortedCategories = data.parentCategories.sort(function(a,b) {
		var valA = a.title ;
		var valB = b.title ;
		
		return valA < valB ? -1 : valA == valB? 0 : 1 ;
	}) ;
	
	if (sortedCategories.length > 0) {
		$.each(sortedCategories, function() {
			var category = $("<a href='?catId=" + this.id + "'>" + this.title + "</a>") ;
			
			var li = $("<li></li>") ;
			li.append(category) ;

			$('#categories').append(li) ;
		}) ;

	} else {
		$('#categories').hide() ;
		$('#noCategories').show() ;
	}
	
	var sortedOutLinks = data.outLinks.sort(function(a,b) {
		var valA = a.title ;
		var valB = b.title ;
		
		return valA < valB ? -1 : valA == valB? 0 : 1 ;
	}) ;
	

	if (sortedOutLinks.length > 0) {
		
		var maxWeight = undefined ;
		var minWeight = undefined ;
		
		$.each(sortedOutLinks, function() {
			var weight = Number(this.relatedness) ;
			
			if (minWeight == undefined || weight < minWeight) minWeight = weight ;
			if (maxWeight == undefined || weight > maxWeight) maxWeight = weight ;
			
		}) ;
		
		$.each(sortedOutLinks, function() {
		
			var link = $("<a pageId='" + this.id + "' href='./?artId=" + this.id + "' relatedness='" + this.relatedness + "'>" + this.title + "</a>") ;
			var weight = Number(this.relatedness) ;
			weight = normalize(weight, minWeight, maxWeight) ;
			link.css('font-size', getFontSize(weight) + "px") ;
			link.css('color', getFontColor(weight)) ;
			
			var li = $("<li></li>") ;
			li.append(link) ;

			$('#linksOut').append(li) ;
		}) ;
		wm_addDefinitionTooltipsToAllLinks($('#linksOut')) ;
	} else {
		$('#linksOut').hide() ;
		$('#noLinksOut').show() ;
	}
	
	
	var sortedInLinks = data.inLinks.sort(function(a,b) {
		var valA = a.title ;
		var valB = b.title ;
		
		return valA < valB ? -1 : valA == valB? 0 : 1 ;
	}) ;
	
	if (sortedInLinks.length > 0) {
		
		var maxWeight, minWeight ;
		$.each(sortedInLinks, function() {
			var weight = Number(this.relatedness) ;
			
			if (minWeight == undefined || weight < minWeight) minWeight = weight ;
			if (maxWeight == undefined || weight > maxWeight) maxWeight = weight ;
		}) ;
		
		$.each(sortedInLinks, function() {
		
			var link = $("<a pageId='" + this.id + "' href='./?artId=" + this.id + "' relatedness='" + this.relatedness + "'>" + this.title + "</a>") ;
			var weight = Number(this.relatedness) ;
			weight = normalize(weight, minWeight, maxWeight) ;
			link.css('font-size', getFontSize(weight) + "px") ;
			link.css('color', getFontColor(weight)) ;
			
			var li = $("<li></li>") ;
			li.append(link) ;

			$('#linksIn').append(li) ;
		}) ;
		wm_addDefinitionTooltipsToAllLinks($('#linksIn')) ;
	} else {
		$('#linksIn').hide() ;
		$('#noLinksIn').show() ;
	}
	
	
}

function processCategoryResponse(data) {
	
	$('#loading').hide() ;
	$('#categoryDetails').show() ;
	
	$('#catTitle').html(data.title) ;
	
	if (data.parentCategories == undefined) {
		$('#parentCats').hide();
		$('#noParentCats').show();
	} else {
		var sortedParentCats = data.parentCategories.sort(function(a,b) {
			var valA = a.title ;
			var valB = b.title ;
			
			return valA < valB ? -1 : valA == valB? 0 : 1 ;
		}) ;
		
		$.each(sortedParentCats, function() {
			var category = $("<a href='?catId=" + this.id + "'>" + this.title + "</a>") ;
			
			var li = $("<li></li>") ;
			li.append(category) ;

			$('#parentCats').append(li) ;
		}) ;
	}
	
	if (data.childCategories == undefined) {
		$('#childCats').hide() ;
		$('#noChildCats').show() ;
	} else {
		var sortedChildCats = data.childCategories.sort(function(a,b) {
			var valA = a.title ;
			var valB = b.title ;
			
			return valA < valB ? -1 : valA == valB? 0 : 1 ;
		}) ;
	
		$.each(sortedChildCats, function() {
			var category = $("<a href='?catId=" + this.id + "'>" + this.title + "</a>") ;
			
			var li = $("<li></li>") ;
			li.append(category) ;

			$('#childCats').append(li) ;
		}) ;
	}
	
	if (data.childArticles == undefined) {
		$('#childArts').hide() ;
		$('#noChildArts').show() ;
	} else {
		var sortedChildArts = data.childArticles.sort(function(a,b) {
			var valA = a.title ;
			var valB = b.title ;
			
			return valA < valB ? -1 : valA == valB? 0 : 1 ;
		}) ;

		$.each(sortedChildArts, function() {
			var link = $("<a pageId='" + this.id + "' href='./?artId=" + this.id + "'>" + this.title + "</a>") ;
			
			var li = $("<li></li>") ;
			li.append(link) ;

			$('#childArts').append(li) ;
		}) ;
	}
	
	wm_addDefinitionTooltipsToAllLinks($('#childArts')) ;
	
}


