package com.stadvdb.group22.mco2.controller;

import com.stadvdb.group22.mco2.model.Movie;
import com.stadvdb.group22.mco2.service.DistributedDBService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;
import org.springframework.web.servlet.view.RedirectView;
import static org.apache.commons.lang3.StringUtils.*;
import static org.apache.commons.lang3.Validate.*;

@Controller
public class UpdateMovieController {

    @Autowired
    private DistributedDBService distributedDBService;

    private String movieUUID;
    private int movieYear;

    @RequestMapping(value = {"movies/y/{movieYear}/m/{movieUUID}", ""}, method = RequestMethod.GET)
    public String getMovie(Model model, @PathVariable String movieUUID, @PathVariable int movieYear) {
        try {
            Movie movie = distributedDBService.getMovieByUUID(movieUUID, movieYear);

            if (movie != null) {
                model.addAttribute("movie", movie);
                this.movieUUID = movieUUID;
                this.movieYear = movieYear;

                return "update_movie";
            } else {
                this.movieUUID = "";
                this.movieYear = -1;

                return "err_movie_not_found";
            }
        } catch (Exception e) {
            this.movieUUID = "";
            this.movieYear = -1;

            e.printStackTrace();
            return "err_database_down";
        }
    }

    @RequestMapping(value = "/update", method = RequestMethod.POST)
    public RedirectView updateMovie(@ModelAttribute Movie movie) {
        notNull (movie);
        notBlank (movie.getTitle());
        notNull (movie.getYear());
        isTrue (movie.getYear() > 0);
        notBlank (movie.getGenre());
        notBlank (movie.getActor1());
        notBlank (movie.getDirector());

        // trim whitespaces and reformat inputs for SQL query
        movie.setTitle(movie.getTitle().trim());
        movie.setGenre(movie.getGenre().trim());
        movie.setActor1(movie.getActor1().trim());
        movie.setActor2(movie.getActor2().trim());
        movie.setDirector(movie.getDirector().trim());

        distributedDBService.updateMovie(movie);

        return new RedirectView ("/");
    }

    @ExceptionHandler(NullPointerException.class)
    public RedirectView handleNullPointerException (RedirectAttributes redirectAttrs) {
        redirectAttrs.addFlashAttribute("errorMsg", "Please fill up all required fields!");
        return new RedirectView("/movies/m/" + this.movieUUID + "/" + this.movieYear);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public RedirectView handleIllegalArgumentException (RedirectAttributes redirectAttrs) {
        redirectAttrs.addFlashAttribute("errorMsg", "Please fill up all required fields!");
        return new RedirectView("/movies/m/" + this.movieUUID + "/" + this.movieYear);
    }
}
