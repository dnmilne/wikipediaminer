
function doEventBindings() {
	$("form").submit(function() {
		var ok = true;
		
		if ($('#term1').val() == "") {
			$('#term1').addClass("ui-state-error") ;
			ok = false ;
		}
		
		if ($('#term2').val() == "") {
			$('#term2').addClass("ui-state-error") ;
			ok = false ;
		}
		
		return ok ;
    });
	
	$('#cmdCompare').button() ;
	
	$('#term1').bind('focus', function() {
		$('#relation').hide('slow') ;
		$('#cmdCompare').show('slow') ;
	}) ;
	
	$('#term2').bind('focus', function() {
		$('#relation').hide('slow') ;
		$('#cmdCompare').show('slow') ;
	}) ;
	
	
	
}

function doTooltipBindings() {
	
	$('#disambiguationHelp').qtip({
	      content: "Words are often ambiguous, and can represent multiple topics. This section explains how one topic is chosen automatically to represent each term.",
		  style: { name: 'wmstyle' }
    }) ;
	
	$('#connectionHelp').qtip({
	      content: "This section lists articles that talk about both of the concepts being compared. These are a useful by-product of how Wikipedia Miner calculates relatedness.",
		  style: { name: 'wmstyle' }
    }) ;
	
	$('#snippetHelp').qtip({
	      content: "This section lists sentences that talk about both of the concepts being compared. These are a useful by-product of how Wikipedia Miner calculates relatedness.",
		  style: { name: 'wmstyle' }
    }) ;
}


$(document).ready(function() {
	
	wm_setHost("../../")
	
	doEventBindings() ;
	doTooltipBindings() ;
	
	
	checkProgress() ;
	
}) ;

function checkProgress() {
	
	$.get(
		"../../services/getProgress",
		{responseFormat:'JSON'},
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
	
	var term1 = urlParams["term1"];
	var term2 = urlParams["term2"];
	
	if (term1 != undefined && term2 != undefined) {
		
		$('#instructions').hide() ;
		$('#loadingSpacer').show() ;
		
		$('#term1').val(term1) ;
		$('#term2').val(term2) ;
		
		$.get(
			"../../services/compare", 
			{
				term1: $('#term1').val(), 
				term2: $('#term2').val(),
				interpretations: true,
				connections: true,
				maxConnectionsReturned: 50,
				snippets: true,
				responseFormat:'JSON'
			},
			function(data){
				processRelatednessResponse(data) ;
			}
		);
		
	} else {
		$('#relation').hide() ;
		$('#cmdCompare').show() ;
	}
}


function processRelatednessResponse(data) {
	
	$('#loadingSpacer').hide() ;
	
	
	var unknownTerm = data.unknownTerm ;
	
	if (unknownTerm != undefined) {

		$('#unkownTerm').html(unknownTerm) ;
		$('#error').show() ;
		
		$('#relation').hide() ;
		$('#cmdCompare').show() ;
		
		return ;
	}
	
	$('#details').show() ;
	
	
	
	$('#relationWeight').html(Math.round(data.relatedness*100) + "% related") ;
	
	var term1 = urlParams["term1"] ;
	var term2 = urlParams["term2"] ;
	
	$('#byTerm1').html(term1) ;
	$('#byTerm2').html(term2) ;
	
	var interpretation = data.disambiguationDetails.interpretations[0] ;
	
	if (interpretation != undefined) {
	
		$('#sense1').html("<a pageId='" + interpretation.id1 + "' href='../search/?artId=" + interpretation.id1 + "'>" + interpretation.title1 + "</a>") ;
		$('#sense2').html("<a pageId='" + interpretation.id2 + "' href='../search/?artId=" + interpretation.id2 + "'>" + interpretation.title2 + "</a>") ;
	
		wm_addDefinitionTooltipsToAllLinks($('#sense1')) ;
		wm_addDefinitionTooltipsToAllLinks($('#sense2')) ;
	} else {
		$('#senses').hide() ;
		$('#noSenses').show() ;
	}
	
	var candidates1 = Number(data.disambiguationDetails.term1Candidates) ;
	var candidates2 = Number(data.disambiguationDetails.term2Candidates) ;
	
	if (candidates1 == 2)
		$('#alternatives1').html("there is <a href='../search/?query=" + term1 + "'>1 other sense</a> for this term.") ;
	else if (candidates1 > 2)
		$('#alternatives1').html("there are <a href='../search/?query=" + term1 + "'>" + (candidates1-1) + " other senses</a> for this term") ;
	else
		$('#alternatives1').html("this is the only possible sense.") ;
		
	if (candidates2 == 2)
		$('#alternatives2').html("there is <a href='../search/?query=" + term2 + "'>1 other sense</a> for this term.") ;
	else if (candidates2 > 2)
		$('#alternatives2').html("there are <a href='../search/?query=" + term2 + "'>" + (candidates2-1) + " other senses</a> for this term") ;
	else
		$('#alternatives2').html("this is the only possible sense.") ;
		
		
	
	var sortedConnections = data.connections.sort(function(a,b) {
		var valA = a.title ;
		var valB = b.title ;
		
		return valA < valB ? -1 : valA == valB? 0 : 1 ;
	}) ;
	
	if (sortedConnections.length > 0) {
		$.each(sortedConnections, function() {		
			var connection = $("<a pageId='" + this.id + "' href='../search/?artId=" + this.id + "'>" + this.title + "</a>") ;
			var weight = (Number(this.relatedness1) + Number(this.relatedness2)) / 2 ;
			connection.css('font-size', getFontSize(weight) + "px") ;
			connection.css('color', getFontColor(weight)) ;
			
			var li = $("<li></li>") ;
			li.append(connection) ;

			$('#connections').append(li) ;
		}) ;
		wm_addDefinitionTooltipsToAllLinks($('#connections')) ;
	} else {
		$('#connections').hide() ;
		$('#noConnections').show() ;
	}
	
	var snippets = data.snippets ;
	
	if (snippets.length > 0) {
		
		$.each(snippets, function() {
			var snippet = $("<ul class='snippet ui-corner-all'><p>" + this.markup + " </p> <p class='source'>from <a pageId='" + this.sourceId + "' href='../search/?artId=" + this.sourceId + "'>" + this.sourceTitle + "</a></ul>") ;
			$("#snippets").append(snippet) ;
		}) ;
	} else {
		$('#snippets').hide() ;
		$('#noSnippets').show() ;
	}
	
	wm_addDefinitionTooltipsToAllLinks($('#snippets')) ;
}