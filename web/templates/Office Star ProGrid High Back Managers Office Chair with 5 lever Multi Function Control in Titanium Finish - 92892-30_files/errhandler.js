
// in case we have any other error handlers on the page
var gOldOnError = window.onerror;

// Override previous handler.
window.onerror = function myErrorHandler(errorMsg, url, lineNumber, columnNumber) {
    var requestUrl = "";

    if (columnNumber === undefined) {
        columnNumber = "-1";
    }

    try {
        if (document.getElementById("config").attributes["requestUrl"] !== undefined) {
            requestUrl = document.getElementById("config").attributes["requestUrl"].value;
        }
    }
    catch (err)
    { }
       
    // serialize errorMsg when it is an event object... (to prevent "[Object event]" being logged in ErrorLog)
    try {
        if (typeof (errorMsg) === 'object') {
            var errorObj = errorMsg;
            errorMsg = '';
            var messageString = '';
            if (errorObj.srcElement) { errorObj = errorObj.srcElement; } //serialize event.srcElement instead if it's available as it won't be serialized
            for (var x in errorObj) {
                if (messageString) messageString += ', ';
                messageString += x + ': ' + errorObj[x];
            }
            errorMsg = '{' + messageString + '}';
        }
    }
    catch (err) {
        errorMsg = "(Cannot read error message.)";
    }

    var bug = new Image();
    bug.src = '/Util/client_side_error.aspx?src=' + escape(document.location.href) +
                                      '&url=' + escape(url) +
                                      '&requestUrl=' + escape(requestUrl) +
                                      '&desc=' + escape(errorMsg + "; file: " + url + "; line_no: " + lineNumber + "; col_no: " + columnNumber) +
                                      '&category=url';
    bug.style.display = 'none';
    // Call previous handler.
    if (gOldOnError) {
        return gOldOnError(errorMsg, url, lineNumber, columnNumber);
    }
    // Just let default handler run.
    return false;
}
