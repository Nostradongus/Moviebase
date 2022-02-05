package com.stadvdb.group22.mco2.controller;

import com.stadvdb.group22.mco2.model.Movie;
import com.stadvdb.group22.mco2.service.MovieService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class HomeController {

    @Autowired
    private MovieService movieService;

    @RequestMapping(value = {"/", ""}, method = RequestMethod.GET)
    public List<Movie> getAllMovies() {
        return movieService.getAllMovies();
    }

    // TODO: temporary, to be removed
    @RequestMapping(value = "/node2", method = RequestMethod.GET)
    public List<Movie> getAllMoviesBefore1980s() {
        return movieService.getAllMoviesBefore1980s();
    }

    // TODO: temporary, to be removed
    @RequestMapping(value = "/node3", method = RequestMethod.GET)
    public List<Movie> getAllMoviesOnAndAfter1980s() {
        return movieService.getAllMoviesOnAndAfter1980s();
    }

}
