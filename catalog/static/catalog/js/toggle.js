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
      seg.addEventListener("click",function(e){
        e.preventDefault();
        e.stopPropagation();
        var newVal=vals[i];
        if(el.getAttribute("data-val")===newVal) return;

        input.value=newVal;
        el.setAttribute("data-val",newVal);

        if(pk){
          var url,fd=new FormData();
          var csrf=getCookie("csrftoken");
          if(!csrf){var ci=document.querySelector("[name=csrfmiddlewaretoken]");if(ci)csrf=ci.value;}
          fd.append("csrfmiddlewaretoken",csrf);

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
            .then(function(r){
              if(!r.ok){console.error("Toggle HTTP",r.status,r.statusText);return r.text().then(function(t){throw new Error("HTTP "+r.status+": "+t.substring(0,200));});}
              return r.json();
            })
            .then(function(data){
              if(data&&!data.ok){
                console.error("Toggle error:",data.error);
                alert("Failed to update: "+(data.error||"unknown error"));
              }
            })
            .catch(function(err){
              console.error("Toggle fetch error:",err);
              alert("Error updating toggle: "+err.message);
            });
        }
      });
    });
  });
});
