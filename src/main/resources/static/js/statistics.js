
    $(document).ready (function () {
        // Statistics URL (/statistics/<URL>/p/x)
        var subURL = "/statistics/" + statisticsURL

        // Reference main container for Pagination
        var paginationList = $("#paginationList");

        // Create "First Page" Button
        var listItemFirstBtn = document.createElement ("li");
        listItemFirstBtn.setAttribute ("class", "page-item");
        listItemFirstBtn.setAttribute ("id", "firstBtnContainer");

        var listItemFirstBtnAnchor = document.createElement ("a");
        listItemFirstBtnAnchor.setAttribute ("class", "page-link");
        listItemFirstBtnAnchor.setAttribute ("href", subURL + "/p/1");
        listItemFirstBtnAnchor.setAttribute ("id", "firstBtn");
        listItemFirstBtnAnchor.setAttribute ("style", "font-weight: bold");
        listItemFirstBtnAnchor.textContent = "<<";

        // Append it to container
        listItemFirstBtn.append (listItemFirstBtnAnchor);
        paginationList.append (listItemFirstBtn);

        // Create "Previous" Button
        var listItemPrevBtn = document.createElement ("li");
        listItemPrevBtn.setAttribute ("class", "page-item");
        listItemPrevBtn.setAttribute ("id", "prevBtnContainer");

        var listItemPrevBtnAnchor = document.createElement ("a");
        listItemPrevBtnAnchor.setAttribute ("class", "page-link");
        listItemPrevBtnAnchor.setAttribute ("href", subURL + "/p/" + (pageNum - 1));
        listItemPrevBtnAnchor.setAttribute ("id", "prevBtn");
        listItemPrevBtnAnchor.textContent = "Previous";

        // Append it to container
        listItemPrevBtn.append (listItemPrevBtnAnchor);
        paginationList.append (listItemPrevBtn);

        // For generating of page buttons
        var i = -1;
        var maxIteration = -1;

        // if first three pages
        if (pageNum < 3) {
            i = 0;
            maxPages > 3 ? maxIteration = 3 : maxIteration = maxPages;
        // if not max page
        } else if (pageNum < maxPages) {
            i = pageNum - 2;
            maxIteration = pageNum + 1;
        // if at max page
        } else {
            maxPages > 3 ? i = pageNum - 3 : i = pageNum - maxPages;
            maxIteration = pageNum;
        }

        // loop to generate buttons
        for (; i < maxPages && i < maxIteration; i++) {
            // Create button
            var listItem = document.createElement ("li");
            listItem.setAttribute ("class", "page-item");

            var listItemAnchor = document.createElement ("a");
            listItemAnchor.setAttribute ("class", "page-link");
            listItemAnchor.setAttribute ("href", subURL + "/p/" + (i + 1));
            listItemAnchor.setAttribute ("id", i + 1);
            listItemAnchor.textContent = i + 1;

            // highlight current page button
            if (i + 1 == pageNum) {
                listItemAnchor.setAttribute ("style", "font-weight: bold");
            }

            // Append the button
            listItem.append (listItemAnchor);
            paginationList.append (listItem);
        }

        // Create "Next" Button
        var listItemNextBtn = document.createElement ("li");
        listItemNextBtn.setAttribute ("class", "page-item");
        listItemNextBtn.setAttribute ("id", "nextBtnContainer");

        var listItemNextBtnAnchor = document.createElement ("a");
        listItemNextBtnAnchor.setAttribute ("class", "page-link");
        listItemNextBtnAnchor.setAttribute ("href", subURL + "/p/" + (pageNum + 1));
        listItemNextBtnAnchor.setAttribute ("id", "nextBtn");
        listItemNextBtnAnchor.textContent = "Next";

        // Append it to container
        listItemNextBtn.append (listItemNextBtnAnchor);
        paginationList.append (listItemNextBtn);

        // Create "Last Page" Button
        var listItemLastBtn = document.createElement ("li");
        listItemLastBtn.setAttribute ("class", "page-item");
        listItemLastBtn.setAttribute ("id", "lastBtnContainer");

        var listItemLastBtnAnchor = document.createElement ("a");
        listItemLastBtnAnchor.setAttribute ("class", "page-link");
        listItemLastBtnAnchor.setAttribute ("href", subURL + "/p/" + maxPages);
        listItemLastBtnAnchor.setAttribute ("id", "lastBtn");
        listItemLastBtnAnchor.setAttribute ("style", "font-weight: bold");
        listItemLastBtnAnchor.textContent = ">>";

        // Append it to container
        listItemLastBtn.append (listItemLastBtnAnchor);
        paginationList.append (listItemLastBtn);

        // for disabling first and previous buttons
        if (pageNum > 1) {
            $("#prevBtnContainer").attr('class', 'page-item');
            $("#firstBtnContainer").attr('class', 'page-item');
        } else {
            $("#prevBtnContainer").attr('class', 'page-item disabled');
            $("#firstBtnContainer").attr('class', 'page-item disabled');
        }

        // for disabling next and last buttons
        if (pageNum == maxPages) {
            $("#nextBtnContainer").attr('class', 'page-item disabled');
            $("#lastBtnContainer").attr('class', 'page-item disabled');
        } else {
            $("#nextBtnContainer").attr('class', 'page-item');
            $("#lastBtnContainer").attr('class', 'page-item');
        }
    });