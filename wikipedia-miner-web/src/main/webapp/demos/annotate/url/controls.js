var linkColor ;



function handleDensityChange(event, ui) {
	var val = ui.value ;
	$('#txtDensity').val(val) ;
	
}

function handleColorChange(event, ui) {
	
	var val = ui.value ;
	linkColor = getColor(val) ;
	
	$('#txtColor').val(val) ;

	//$('#colorSlider').find('.ui-slider-handle').css('border-color', linkColor) ;
	
	$('#colorSlider').find('.ui-slider-handle').css('background-position', (Math.floor(-100*val)-25) + "px 0px") ;
}

function doUiBindings() {
	$('#cmdAnnotate').button() ;

	$('#colorSlider').slider(
		{
			max: 1,
			step:0.01,
			slide: handleColorChange,
			change: handleColorChange
		}
	) ;
	
	$('#densitySlider').slider(
		{
			max: 1,
			step:0.01,
			slide: handleDensityChange,
			change: handleDensityChange
		}
	) ;
	
}

function setOptionValues() {

	var source = "" ;
	var density = 0.5 ;
	var color = 0.5 ;

	if (urlParams["source"] != undefined)
		source = urlParams["source"] ;
	
	if (urlParams["linkDensity"] != undefined)
		density = Number(urlParams["linkDensity"]) ;
		
	if (urlParams["linkColor"] != undefined)
		color = Number(urlParams["linkColor"]) ;
	
	$("#txtSource").val(source) ;
	$('#densitySlider').slider("value", density) ;				
	$('#colorSlider').slider("value", color) ;
	
}

$(document).ready(function() {					

	doUiBindings() ;
	setOptionValues() ;
	
	$(window).trigger('resize') ;
}) ;

$(window).resize(function() {
	var w = $(window).width() ;
	
	$('#txtSource').width(w-790) ;
}) ;
