

var datatypeDescriptions = {} ;
datatypeDescriptions["integer"] = "any whole number" ;
datatypeDescriptions["float"] = "any whole number or fraction" ;
datatypeDescriptions["string"] = "a URL-encoded string" ;
datatypeDescriptions["boolean"] = "<i>true</i> or <i>false</i>. If the name of a boolean parameter is given without a value, then the value is assumed to be true" ;
datatypeDescriptions["integer list"] = "a list of comma separated integers (e.g. <i>12,14,200</i>)" ;
datatypeDescriptions["enum"] = "one of a prespecified set of strings" ;
datatypeDescriptions["enum list"] = "a comma separated set of enums" ;


var currService ;

$(document).ready(function() {
	//requestServiceList() ;
	
	var location = document.location.toString() ;
	if (location.match('\\?')) 
		currService = location.split('?')[1] ;
		
	requestServiceList() ;
	
	if (currService != undefined) {
		requestServiceDetails(currService) ;
		
		
		$("#serviceIntro").hide() ;
		$("#serviceDetails").show() ;
	} else {
		wm_addDefinitionTooltipsToAllLinks($('#allServices')) ;
		
		$("#serviceIntro").show() ;
		$("#serviceDetails").hide() ;
	}
}) ;

function requestServiceList() {
	$.get(
		"listServices", 
		{responseFormat:'JSON'},
		function(data){
			processServiceListResponse(data);
		}
	);
}

function processServiceListResponse(data) {
	
	var serviceList = $("#serviceList") ;
	
	if (currService == undefined) {
		serviceList.append("<li><em>introduction</em></li>") ;
	} else {
		serviceList.append("<li><a href='.'>introduction</a></li>") ;
	}

	$.each(data.serviceGroups, function() {
				
		serviceList.append("<li class='header'><em>" + this.name + "</em> services</li>") ;
		
		$.each(this.services, function(name, description) {
				
			if (name == currService) {
				serviceList.append("<li><em>" + name + "</em></li>") ;
			} else {
					
				var service = $("<li><a href='?" + name + "'>" + name + "</a></li>") ;
				service.qtip(
				{
				      content: description,
					  position: {
					  	my: 'left center',
					   	at: 'right center',
						adjust: { x: 5 }
					  }
				 }) ;
						
				serviceList.append(service) ;
			}
		}) ;
		
	}) ;
	
}

function requestServiceDetails(serviceName) {
	$.get(
		serviceName, 
		{
			help:true,
			responseFormat:'JSON'
		},
		function(data){
			processServiceDetailsResponse(data, serviceName);
		}
	);
}

function processServiceDetailsResponse(data, serviceName) {
	
	var desc = data.serviceDescription ;
	
	$("#serviceDetails").append("<h2><em>" + serviceName + "</em> service</h2>") ;
	
	$("#serviceDetails").append(desc.details) ;
	
	if (desc.examples.length > 0) {
		$("#serviceDetails").append("<h3>Examples</h3>") ;
		
		$.each(desc.examples , function() {
			$("#serviceDetails").append(constructExampleBox(this,  desc.name));
		}) ;
	}
	
	$("#serviceDetails").append("<h3>Available parameters</h3>") ;
	
	//if ($(response).find("Parameter[name!='help']").length == 0) {
	//	$("#serviceDetails").append("<p class='explanatory'>There are no parameters to specify</p>") ;
	//	return ;
	//}
	
	var paramGroups = desc.parameterGroups ;
	
	if (paramGroups.length > 0) {
		
		$("#serviceDetails").append("<p class='explanatory'>Only specify parameters from <em>one</em> of the following groups</p>") ;
		
		$.each(paramGroups, function(){
			$("#serviceDetails").append(constructParamGroupBox(this)) ;
		}) ;
	}
	
	var mandatoryGlobalParams = new Array() ;
	var optionalGlobalParams = new Array() ;
	
	$.each(desc.globalParameters, function() {
		if (this.optional == true)
			optionalGlobalParams.push(this) ;
		else
			mandatoryGlobalParams.push(this) ;
	}) ;
	

	if (mandatoryGlobalParams.length > 0) {
		
		if (mandatoryGlobalParams.length == 1)
			$("#serviceDetails").append("<p class='explanatory'>Specify the following</p>") ;
		else
			$("#serviceDetails").append("<p class='explanatory'>Specify <em>all</em> of the following</p>") ;
		
		$.each(mandatoryGlobalParams, function(){
			var divParam = constructParamBox(this) ;
			$("#serviceDetails").append(divParam);
			intializeParamBox(divParam, this) ;
		}) ;
	}
	
	var baseParams = desc.baseParameters ;
	
	if (optionalGlobalParams.length + baseParams.length > 0) {
		
		if (optionalGlobalParams.length + baseParams.length == 1)
			$("#serviceDetails").append("<p class='explanatory'>Optionally specify the following</p>") ;
		else 
			$("#serviceDetails").append("<p class='explanatory'>Optionally specify <em>any</em> of the following</p>") ;
		
		$.each(optionalGlobalParams, function(){
			var divParam = constructParamBox(this) ;
			$("#serviceDetails").append(divParam);
			intializeParamBox(divParam, this) ;
		}) ;
		
		$.each(baseParams, function(){
			var divParam = constructParamBox(this) ;
			$("#serviceDetails").append(divParam);
			intializeParamBox(divParam, this) ;
		}) ;
	}
}

function constructParamGroupBox(data) {
	var divParamGroup = $("<div class='paramGroup ui-widget-content ui-corner-all'></div>") ;
	
	var divParamGroupTitle = $("<div class='title'></div>") ;
	divParamGroup.append(divParamGroupTitle) ;
	
	divParamGroupTitle.append("<div style='float:left' class='ui-icon ui-icon-circle-triangle-s'></div>") ;
	divParamGroupTitle.append("<div style='float:left ; display:none ;' class='ui-icon ui-icon-circle-triangle-n'></div>") ;
	
	divParamGroupTitle.bind('click', function() {
		$(this).parent().children('.content').slideToggle('fast')  ;
		$(this).children('.ui-icon').toggle() ;
		return false ;
	}) ;
	
	divParamGroupTitle.bind('mouseover', function(event) {
		$(this).parent().addClass("ui-state-highlight") ;
	}) ;
	
	divParamGroupTitle.bind('mouseout', function(event) {
		$(this).parent().removeClass("ui-state-highlight") ;
	}) ;
	
	divParamGroupTitle.append("<span>" + data.description + "</span>");
	
	var divParamGroupContent = $("<div class='content' style='display: none'></div>") ;
	divParamGroup.append(divParamGroupContent) ;
	
	
	var mandatoryParams = new Array() ;
	var optionalParams = new Array() ;
	
	$.each(data.parameters, function() {
		if (this.optional == true)
			optionalParams.push(this) ;
		else
			mandatoryParams.push(this) ;
	}) ;
	
	if (mandatoryParams.length > 0) {
		if (mandatoryParams.length == 1)
			divParamGroupContent.append("<p class='explanatory'>Specify the following</p>");
		else
			divParamGroupContent.append("<p class='explanatory'>Specify <em>all</em> of the following</p>");
		
		$.each(mandatoryParams, function(){
			var divParam = constructParamBox(this) ;
			divParamGroupContent.append(divParam);
			intializeParamBox(divParam, this) ;
		});
	}
	
	if (optionalParams.length > 0) {
		if (optionalParams.length == 1)
			divParamGroupContent.append("<p class='explanatory'>Optionally specify the following</p>");
		else
			divParamGroupContent.append("<p class='explanatory'>Optionally specify <em>any</em> of the following</p>");
		
		$.each(optionalParams, function(){
			var divParam = constructParamBox(this) ;
			divParamGroupContent.append(divParam);
			intializeParamBox(divParam, this) ;
		});
	}
	
	return divParamGroup ;
}

function constructParamBox(data) {
	
	var divParam = $("<div class='param ui-widget-content ui-corner-all'></div>");
	
	var divParamTitle = $("<div class='title' ></div>") ;
	divParam.append(divParamTitle) ;

	
	divParamTitle.append("<div style='float:left' class='ui-icon ui-icon-circle-triangle-s'></div>") ;
	divParamTitle.append("<div style='float:left' class='ui-icon ui-icon-circle-triangle-n'></div>") ;
	
	divParamTitle.bind('click', function(event) {
		$(this).parent().children('.content').slideToggle('fast') ;
		$(this).children('.ui-icon').toggle() ;
		event.stopImmediatePropagation();
	}) ;
	
	
	divParamTitle.bind('mouseover', function(event) {
		$(this).parent().addClass("ui-state-highlight") ;
	}) ;
	
	divParamTitle.bind('mouseout', function(event) {
		$(this).parent().removeClass("ui-state-highlight") ;
	}) ;
	
	
	divParamTitle.append("<span>" + data.name + "</span>");
	
	var divParamContent = $("<div class='content'></div>") ;
	divParam.append(divParamContent) ;
	divParamContent.append("<p>" + data.description + "</p>") ;
	
	var tblType = divParamContent.append("<table></table>") ;
	
	var tr = tblType.append("<tr></tr>") ;
	tr.append("<td class='header'>type</td>") ;
	
	tr.append("<td><a class='datatype'>" + data.datatype + "</a></td>") ;
	tr.find(".datatype").qtip(
	   {
	      content: datatypeDescriptions[data.datatype],
		  style: {
              name: 'wmstyle' 
   		  }
	   }) ;
	
	if (data.possibleValues != undefined) {
		
		tr = tblType.append("<tr/>") ;
		
		tr.append("<td class='header'>possible values</td>") ;
		
		var td = $("<td></td>") ;
	
		$.each(data.possibleValues, function(name, desc) {

			var val = $("<span class='value'>" + name + "</a></span>") ;
			val.qtip(
			   {
			      content: desc,
				  style: {
	                  name: 'wmstyle' 
           		  }
			   }) ;
			td.append(val) ;
		}) ;
		
		tr.append(td) ;
	}
	
	
	if (typeof data.defaultValue != 'undefined') {
		tr = tblType.append("<tr></tr>") ;
		
		tr.append("<td class='header'>default value</td>")
		tr.append("<td>" + data.defaultValue + "</td>");	
		
	}
	return divParam ;
}

//Decide wheither to initially expand or collapse param box initially
//Mandatory params are expanded initially, optional ones are collapsed
function intializeParamBox(divParam, data) {
	if (typeof data.defaultValue != 'undefined') {
		divParam.find(".content").hide() ;
		divParam.find(".ui-icon-circle-triangle-s").show() ;
		divParam.find(".ui-icon-circle-triangle-n").hide() ;	
	} else {
		divParam.find(".content").show() ;
		divParam.find(".ui-icon-circle-triangle-s").hide() ;
		divParam.find(".ui-icon-circle-triangle-n").show() ;
	}
}



function constructExampleBox(data, serviceName) {
	
	var divExample = $("<div class='example ui-widget-content ui-corner-all'></div>");
	
	var divExampleTitle = $("<div class='title' ></div>") ;
	divExample.append(divExampleTitle) ;
	
	divExampleTitle.append("<div style='float:left' class='ui-icon ui-icon-circle-triangle-s'></div>") ;
	divExampleTitle.append("<div style='float:left; display:none' class='ui-icon ui-icon-circle-triangle-n'></div>") ;
	
	divExampleTitle.bind('click', function(event) {
		$(this).parent().children('.content').slideToggle('fast') ;
		$(this).children('.ui-icon').toggle() ;
	}) ;
	
	divExampleTitle.bind('mouseover', function(event) {
		$(this).parent().addClass("ui-state-highlight") ;
	}) ;
	
	divExampleTitle.bind('mouseout', function(event) {
		$(this).parent().removeClass("ui-state-highlight") ;
	}) ;
	
	divExampleTitle.append("<span class='title'>" + data.description + "</span>") ;
	
	var divExampleContent = $("<div class='content'></div>") ;
	divExample.append(divExampleContent) ;
	
	
	
	var parParams = $("<p class='params'>services/" + serviceName + "</p>") ;
	
	divExampleContent.append(parParams) ;
	
	var index = 0 ;
	$.each(data.parameters, function(name, value) {
		
		var parParam = $("<p class='param'></p>") ;
		parParams.append(parParam) ;
		
		if (index == 0)
			parParam.append("?") ;
		else
			parParam.append("&") ;
			
		if (value == 'true')
			parParam.append("<b>" + name + "</b>") ;
		else
			parParam.append("<b>" + name + "</b>=" + value) ;
			
		index++ ;
	});
	
	var cmdTry = $("<a class='try' target='_blank' href='" + data.url + "'>try it!</a>") ;
	divExampleContent.append(cmdTry) ;
	
	return divExample ;
}