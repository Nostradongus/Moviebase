package com.stadvdb.group22.mco2.controller;

import com.stadvdb.group22.mco2.model.Movie;
import com.stadvdb.group22.mco2.service.DistributedDBService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.view.RedirectView;

import java.util.UUID;

@Controller
public class AddMovieController {

    @Autowired
    private DistributedDBService distributedDBService;

    @RequestMapping(value = "/add_movie", method = RequestMethod.GET)
    public String getAddMoviePage(Model model) {
        model.addAttribute("movie", new Movie ());

        return "add_movie";
    }

    @RequestMapping(value = "/add", method = RequestMethod.POST)
    public RedirectView addMovie(@ModelAttribute Movie movie) {
        // TODO: add input validation (e.g. null / blank input, etc.)

        // trim whitespaces and reformat inputs for SQL query
        movie.setTitle(movie.getTitle().trim());
        movie.setGenre(movie.getGenre().trim());
        movie.setActor1(movie.getActor1().trim());
        movie.setActor2(movie.getActor2().trim());
        movie.setDirector(movie.getDirector().trim());
        movie.setUuid(UUID.randomUUID().toString());

        try {
            distributedDBService.addMovie(movie);
            return new RedirectView ("/");
        } catch (Exception e) {
            // TODO: handle exception here for front-end
            return null;
        }
    }
}
