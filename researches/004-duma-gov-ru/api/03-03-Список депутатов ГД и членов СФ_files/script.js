actualizeForm = function() {
  var currentValue = $('#site_status').val();
  if (currentValue == 'site') {
    $('.site').show();
    $('.app').hide();
  }
  else {
    $('.site').hide();
    $('.app').show();
  }
}

$(document).ready(function (){

  var isIOS = navigator.platform.match(/(ipad|ipod|iphone)/i) != null;
  if (isIOS)
    $('body').addClass('ios');
   
/*~~~~~~‘окус дл€ полей ввода~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

  $('form input[type=text]:not(.ft-disabled),form textarea,form input[type=password]').focus(
    function () {
      if ($(this).val() == $(this).attr('title')){
        $(this).val('');
      }
    }
  );
  $('form input[type=text]:not(.ft-disabled),form textarea,form input[type=password]').blur(
    function () {
      if (this.value == ''){
        $(this).val($(this).attr('title'));
      }
    }
  );

  if (($.browser.msie) && ($.browser.version < 8)) {
     $('.form-text-big,.form-text')
     .focus(
       function() {
         $(this).css({
           'background':'#ffffe6'
         })
       }
     )
     .blur(
       function() {
         $(this).css({
           'background':'#fff'
         })
       }
     )
  }
  
  /*~~~~~~~~~~~~ кнопки down-up ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

  function btnMouseDown(btnDownClass) {
    $('.'+btnDownClass).live('mousedown',
      function() {
        $(this).addClass(btnDownClass+'-down')
      }
    )
    .mouseup(
    );
    $('.'+btnDownClass).live('mouseup',
      function() {
        $(this).removeClass(btnDownClass+'-down')
      }
    )
    .mouseleave(
    );
    $('.'+btnDownClass).live('mouseleave',
      function() {
        $(this).removeClass(btnDownClass+'-down')
      }
    );
  }

  var arrayBtnDownClass = ['submit-big','submit'];

  for (i=0;i<arrayBtnDownClass.length;i++) {
    btnMouseDown (arrayBtnDownClass[i])
  }

  $('.submit-disabled').click(
    function() {
      return false
    }
  );

/*~~~~~~~~~~~~ парамеиры поиска ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
  $('.param-select').selectboxParam();

/* подсказка  к вопрсику */
  setupQuestTitle = function(oTitle){
    var questBox = document.createElement("span");
    var questBoxT = document.createElement("span");
    var questBoxB = document.createElement("span");
    $questBox = jQuery(questBox);
    $questBoxT = jQuery(questBoxT);
    $questBoxB = jQuery(questBoxB);
    $questBox.addClass('quest-title');
    $questBoxT.addClass('quest-title-t');
    $questBoxB.addClass('quest-title-b');
	$questBox.text(oTitle);
	
	return $questBox.append($questBoxT).prepend($questBoxB);
  }

  $('.quest').hover(
    function() {
	  $questTitle = setupQuestTitle($(this).attr('title'));
	  $questTitle.css({
	    'left':$(this).position().left-7,'top':$(this).position().top+23
	  });
	  $(this).attr('title','');
	  $(this).after($questTitle);
	},
    function() {
	  $questTitle.hide('50');
	  $(this).attr('title',$questTitle.text());
	  $questTitle.remove();
	}
  );
  
/*~~~~~~~~~~~~  нопка получить код ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
  $('#gfk-check input').removeAttr('checked');
  $('#gfk-check').click(
    function() {
	  if ($(this).children('input').is(':checked')) {
	    $(this).parent().prev().removeClass('submit-big-disabled');
		$(this).parent().prev().find('input').removeAttr('disabled');
	  } else {
	    $(this).parent().prev().addClass('submit-big-disabled');
		$(this).parent().prev().find('input').attr('disabled','disabled');
	  }
    }
  );

  $('#site_status').change(actualizeForm);  
  actualizeForm();  

  if ($('#btn-copy').length) {
    swfobject.embedSWF(
      '/img/copy-btn.swf', 
      "btn-copy",
      "230", "30", "8.0.0","", 
      {}, { allowScriptAccess: 'always', wmode: 'opaque' },
      {}
    );      
  }

  getCopiedStringForFlashButton = function() {
    return $('#copied-text').text().replace(/(\s{5,})/, "\n");
  }
    
}); //end
