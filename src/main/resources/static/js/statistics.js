
$(document).ready (function () {
    var subURL = "/statistics/" + statisticsURL

    if (pageNum < 3) {
        $("#page1").text("1");
        $("#page1").attr('href', subURL + '/p/1');
        $("#page2").text("2");
        $("#page2").attr('href', subURL + '/p/2');
        $("#page3").text("3");
        $("#page3").attr('href', subURL + '/p/3');
    } else if (pageNum < maxPages) {
        $("#page1").text(pageNum - 1);
        $("#page1").attr('href', subURL + '/p/' + (pageNum - 1));
        $("#page2").text(pageNum);
        $("#page2").attr('href', subURL + '/p/' + (pageNum));
        $("#page3").text(pageNum + 1);
        $("#page3").attr('href', subURL + '/p/' + (pageNum + 1));
    } else {
        $("#page1").text(maxPages - 2);
        $("#page1").attr('href', subURL + '/p/' + (maxPages - 2));
        $("#page2").text(maxPages - 1);
        $("#page2").attr('href', subURL + '/p/' + (maxPages - 1));
        $("#page3").text(maxPages);
        $("#page3").attr('href', subURL + '/p/' + maxPages);
    }

    if (pageNum > 1) {
        $("#prevBtnContainer").attr('class', 'page-item');
    } else {
        $("#prevBtnContainer").attr('class', 'page-item disabled');
    }

    if (pageNum == maxPages) {
        $("#nextBtnContainer").attr('class', 'page-item disabled');
    } else {
        $("#nextBtnContainer").attr('class', 'page-item');
    }

    $("#prevBtn").attr('href', subURL + '/p/' + (pageNum - 1));
    $("#nextBtn").attr('href', subURL + '/p/' + (pageNum + 1));
});