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
    for (var i = 0; i < items.length; ++i) {
        content += "<p><b>" + items[i].time + ":</b> " + items[i].update + "</p>\n";
    }

    document.getElementById("content").innerHTML = content;
    window.setTimeout(showStuff, 500);
}

window.onload = showStuff;
