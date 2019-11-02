window.onload = function() {
  setTimeout(injecthtml,500);
};

function injecthtml(){
 var spans = document.getElementsByTagName('span');
	if(spans){
	for (var i = 0; i < spans.length; i++) {
	  var span = spans[i];
	  if(span && span.childElementCount>1 && span.innerHTML.indexOf("tag_run_id") > -1){
debugger;
		var run_id = span.children[1].innerHTML;
		span.children[0].innerHTML = "<a target='_blank' href='https://www.google.com/?q=" + run_id + "' >Promote</a>";
		span.children[1].remove();
	  }
	}
     }
}
