package com.stadvdb.group22.mco2.controller;

import com.stadvdb.group22.mco2.model.Movie;
import com.stadvdb.group22.mco2.service.DistributedDBService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.view.RedirectView;

@Controller
public class UpdateMovieController {

    @Autowired
    private DistributedDBService distributedDBService;

    @RequestMapping(value = {"movies/m/{movieUUID}/{movieYear}", ""}, method = RequestMethod.GET)
    public String getMovie(Model model, @PathVariable String movieUUID, @PathVariable int movieYear) {
        try {
            Movie movie = distributedDBService.getMovieByUUID(movieUUID, movieYear);

            if (movie != null) {
                model.addAttribute("movie", movie);
                return "update_movie";
            } else {
                return "movie_not_found";
            }
        } catch (Exception e) {
            e.printStackTrace();
            // TODO: Redirect to "database down"
            return "movie_not_found";
        }
    }

    @RequestMapping(value = "/update", method = RequestMethod.POST)
    public RedirectView updateMovie(@ModelAttribute Movie movie) {
        // TODO: add input validation (e.g. null / blank input, etc.)

        // trim whitespaces and reformat inputs for SQL query
        movie.setTitle(movie.getTitle().trim());
        movie.setGenre(movie.getGenre().trim());
        movie.setActor1(movie.getActor1().trim());
        movie.setActor2(movie.getActor2().trim());
        movie.setDirector(movie.getDirector().trim());

        distributedDBService.updateMovie(movie);

        return new RedirectView ("/");
    }
}
