$(document).ready(function () {
    $(".cymax-click-event").click(function () { myEventHandler(this, "click"); });
    $(".cymax-input-event").change(function () { myEventHandler(this, "change"); });
    $(".cymax-load-event").each(function () { myEventHandler(this, "load"); });
});

function myEventHandler(evtSrc, action) {
    if (action === undefined) {
        action = "";
    }

    console.log(action);

    var src = escape(document.location.href);
    console.log(src);

    var item = escape($(evtSrc).attr("id"));
    if (item === "undefined") {
        item = escape($(evtSrc).prop("tagName"));
    }
    console.log(item);

    var msg = escape($(evtSrc).attr("data-eventmessage"));
    if (msg === "undefined") {
        msg = "";
    }
    if (action == "change") {
        if (msg == "") {
            msg = item + " : ";
        }
        else {
            msg += " : ";
        }
        var evtInput = evtSrc;
        if (evtInput.tagName != "INPUT" && evtInput.tagName != "SELECT") {
            evtInput = $(evtSrc).find("input")[0] || $(evtSrc).find("select")[0];
        }
        if (evtInput.type == "checkbox") {
            msg += evtInput.checked;
        }
        else {
            msg += evtInput.value;
        }
    }
    msg = unescape(msg);
    console.log(msg);

    var ev = new Image();
    ev.src = '/Util/client_side_event.aspx?src=' + src +
                                      '&item=' + item +
                                      '&desc=' + msg +
                                      '&action=' + escape(action);
    ev.style.display = 'none';

    // Just let other handlers run.
    return false;
}