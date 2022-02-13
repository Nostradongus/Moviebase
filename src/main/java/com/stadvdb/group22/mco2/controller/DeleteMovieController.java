package com.stadvdb.group22.mco2.controller;

import com.stadvdb.group22.mco2.model.Movie;
import com.stadvdb.group22.mco2.service.DistributedDBService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.view.RedirectView;

@Controller
public class DeleteMovieController {

    @Autowired
    private DistributedDBService movieService;

    @RequestMapping(value = "/delete/{movieUUID}", method = RequestMethod.GET)
    public RedirectView deleteMovie(@PathVariable String movieUUID) {
        System.out.println ("MOVIE ID: " + movieUUID);
        Movie movie = new Movie ();
        movie.setUuid(movieUUID);
        movieService.deleteMovie(movie);

        return new RedirectView ("/");
    }
}
