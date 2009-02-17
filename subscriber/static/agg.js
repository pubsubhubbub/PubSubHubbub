function showStuff() {
  var req = new XMLHttpRequest();
  req.open("GET", "/items", true);
  req.onreadystatechange = function() {
    if (req.readyState != 4) {
      return;
    }
    gotStuff(req.status, req.responseText);
  };
  req.send(null);
}

function gotStuff(status, text) {
  if (status != 200) {
    window.setTimeout(showStuff, 5000);
    return;
  }

  var content = "";
  var items = eval(text);
  if (items.length == 0) {
    content = "Nothing yet.\n"
  } else {
    for (var i = 0; i < items.length; ++i) {
      content += "<div class='entry'><div><strong>" + items[i].title +
          "</strong> at <em>" + items[i].time + "</em></div>" +
          "<div>from <a href='" + items[i].source + "'>" + items[i].source +
          "</a></div><div>" + items[i].content + "</div></div>\n";
    }
  }

  document.getElementById("content").innerHTML = content;
  window.setTimeout(showStuff, 500);
}

window.onload = showStuff;
