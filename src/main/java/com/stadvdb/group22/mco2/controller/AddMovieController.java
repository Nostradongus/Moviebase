package com.stadvdb.group22.mco2.controller;

import com.stadvdb.group22.mco2.model.Movie;
import com.stadvdb.group22.mco2.service.DistributedDBService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;
import org.springframework.web.servlet.view.RedirectView;

import java.util.UUID;

import static org.apache.commons.lang3.Validate.*;
import static org.apache.commons.lang3.Validate.notBlank;

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
        movie.setUuid(UUID.randomUUID().toString());

        try {
            distributedDBService.addMovie(movie);
            return new RedirectView ("/");
        } catch (Exception e) {
            // TODO: handle exception here for front-end
            return null;
        }
    }

    @ExceptionHandler(NullPointerException.class)
    public RedirectView handleNullPointerException (RedirectAttributes redirectAttrs) {
        redirectAttrs.addFlashAttribute("errorMsg", "Please fill up all required fields!");
        return new RedirectView("/add_movie");
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public RedirectView handleIllegalArgumentException (RedirectAttributes redirectAttrs) {
        redirectAttrs.addFlashAttribute("errorMsg", "Please fill up all required fields!");
        return new RedirectView("/add_movie");
    }
}
