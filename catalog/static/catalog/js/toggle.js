document.addEventListener("DOMContentLoaded",function(){
  var vals=["-1","0","1"];

  function getCookie(name){
    var v=document.cookie.match("(^|;)\\s*"+name+"\\s*=\\s*([^;]+)");
    return v?v.pop():"";
  }

  document.querySelectorAll(".tri-toggle").forEach(function(el){
    var input=el.querySelector("input[type=hidden]");
    var pk=el.getAttribute("data-pk");
    var field=el.getAttribute("data-field");

    el.querySelectorAll(".tri-toggle__seg").forEach(function(seg,i){
      seg.addEventListener("click",function(){
        var newVal=vals[i];
        if(el.getAttribute("data-val")===newVal) return;

        input.value=newVal;
        el.setAttribute("data-val",newVal);
        input.dispatchEvent(new Event("change",{bubbles:true}));

        if(pk){
          var url,fd=new FormData();
          fd.append("csrfmiddlewaretoken",getCookie("csrftoken"));

          if(field){
            // Generic field toggle (is_manufacturer, etc.)
            url="/django-admin/catalog/manufacturer/set-field/"+pk+"/";
            fd.append("field",field);
            fd.append("value",newVal);
          }else{
            // Legacy profile status toggle
            url="/django-admin/catalog/manufacturer/set-status/"+pk+"/";
            fd.append("status",newVal);
          }

          fetch(url,{method:"POST",body:fd,credentials:"same-origin"})
            .then(function(r){return r.json();})
            .then(function(data){
              if(!data.ok){
                console.error("Toggle error:",data.error);
                alert("Failed to update: "+(data.error||"unknown error"));
              }
            })
            .catch(function(err){
              console.error("Toggle fetch error:",err);
              alert("Network error updating toggle.");
            });
        }
      });
    });
  });
});
