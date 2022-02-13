
$(document).ready (function () {
    //var pageNum = parseInt ($("#pageNum").val());
    //var maxPages = parseInt ($("#maxPages").val());
    //alert (maxPages);
    //alert (pageNum);
    //console.log (pageNum);


    if (pageNum < 3) {
        $("#page1").text("1");
        $("#page1").attr('href', '/movies/p/1');
        $("#page2").text("2");
        $("#page2").attr('href', '/movies/p/2');
        $("#page3").text("3");
        $("#page3").attr('href', '/movies/p/3');
        $("#page4").text("4");
        $("#page4").attr('href', '/movies/p/4');
        $("#page5").text("5");
        $("#page5").attr('href', '/movies/p/5');
    } else if (pageNum < maxPages) {
        $("#page1").text(pageNum - 2);
        $("#page1").attr('href', '/movies/p/' + (pageNum - 2));
        $("#page2").text(pageNum - 1);
        $("#page2").attr('href', '/movies/p/' + (pageNum - 1));
        $("#page3").text(pageNum);
        $("#page3").attr('href', '/movies/p/' + (pageNum));
        $("#page4").text(pageNum + 1);
        $("#page4").attr('href', '/movies/p/' + (pageNum + 1));
        $("#page5").text(pageNum + 2);
        $("#page5").attr('href', '/movies/p/' + (pageNum + 2));
    } else {
        $("#page1").text(maxPages - 4);
        $("#page1").attr('href', '/movies/p/' + (maxPages - 4));
        $("#page2").text(maxPages - 3);
        $("#page2").attr('href', '/movies/p/' + (maxPages - 3));
        $("#page3").text(maxPages - 2);
        $("#page3").attr('href', '/movies/p/' + (maxPages - 2));
        $("#page4").text(maxPages - 1);
        $("#page4").attr('href', '/movies/p/' + (maxPages - 1));
        $("#page5").text(maxPages);
        $("#page5").attr('href', '/movies/p/' + maxPages);
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

    $("#prevBtn").attr('href', '/movies/p/' + (pageNum - 1));
    $("#nextBtn").attr('href', '/movies/p/' + (pageNum + 1));
});