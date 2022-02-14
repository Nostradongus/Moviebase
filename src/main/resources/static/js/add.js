$(document).ready (function () {
    $("#addMovieForm").submit(function (e) {
        if (!isValidForm ())
            e.preventDefault ();
    })

    function resetBorders () {
        $("#title").css ("border", "solid 2px gray");
        $("#year").css ("border", "solid 2px gray");
        $("#genre").css ("border", "solid 2px gray");
        $("#actor1").css ("border", "solid 2px gray");
        $("#director").css ("border", "solid 2px gray");
        $("#errorMessage").text ("");
    }

    function isValidForm () {
        var title = $("#title").val().trim();
        var year = $("#year").val().trim();
        var genre = $("#genre").val().trim();
        var actor1 = $("#actor1").val().trim();
        var director = $("#director").val().trim();

        resetBorders ();

        if (title.length == 0) {
            $("#title").css ("border-color", "tomato");
            $("#errorMessage").text ("Title cannot be empty!");
            return false;
        } else if (year.length == 0 || year < 0) {
            $("#year").css ("border-color", "tomato");
            $("#errorMessage").text ("Enter a valid year!");
            return false;
        } else if (genre.length == 0) {
            $("#genre").css ("border-color", "tomato");
            $("#errorMessage").text ("Genre cannot be empty!");
            return false;
        } else if (actor1.length == 0) {
            $("#actor1").css ("border-color", "tomato");
            $("#errorMessage").text ("Actor #1 cannot be empty!");
            return false;
        } else if (director.length == 0) {
            $("#director").css ("border-color", "tomato");
            $("#errorMessage").text ("Director cannot be empty!");
            return false;
        } else {
            return true;
        }
    }

    // For clearing of fields
    $("#clearBtn").click (function () {
        $("#title").val("");
        $("#year").val("");
        $("#genre").val("");
        $("#actor1").val("");
        $("#actor2").val("");
        $("#director").val("");
    })
})