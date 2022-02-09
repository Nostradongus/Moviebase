package com.stadvdb.group22.mco2.controller;

import com.stadvdb.group22.mco2.model.Movie;
import com.stadvdb.group22.mco2.model.Report;
import com.stadvdb.group22.mco2.service.MovieService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;

import java.util.List;

@RestController
public class HomeController {

    @Autowired
    private MovieService movieService;

    @RequestMapping(value = {"/", ""}, method = RequestMethod.GET)
    public Page<Movie> getMovies(@RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "5") int size) {
        return movieService.getMoviesByPage(page, size);
    }

    @RequestMapping(value = "/search", method = RequestMethod.GET)
    public Page<Movie> searchMovies(@RequestParam(defaultValue = "") String year, @RequestParam(defaultValue = "") String title,
                                   @RequestParam(defaultValue = "") String genre, @RequestParam(defaultValue = "") String actor,
                                   @RequestParam(defaultValue = "") String director, @RequestParam(defaultValue = "0") int page,
                                   @RequestParam(defaultValue = "5") int size) {
        // trim whitespaces and reformat inputs for SQL query
        year = !year.equalsIgnoreCase("") ? year.trim() : null;
        title = !title.equalsIgnoreCase("") ? "'" + title.trim() + "'" : null;
        genre = !genre.equalsIgnoreCase("") ? "'" + genre.trim() + "'" : null;
        actor = !actor.equalsIgnoreCase("") ? "'" + actor.trim() + "'" : null;
        director = !director.equalsIgnoreCase("") ? "'" + director.trim() + "'" : null;

        return movieService.searchMoviesByPage(year, title, genre, actor, director, page, size);
    }

    @RequestMapping(value = "/movies_per_genre", method = RequestMethod.GET)
    public Page<Report> getMoviesPerGenre(@RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "5") int size) {
        return movieService.getMoviesPerGenreByPage(page, size);
    }

    @RequestMapping(value = "/movies_per_director", method = RequestMethod.GET)
    public Page<Report> getMoviesPerDirector(@RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "5") int size) {
        return movieService.getMoviesPerDirectorByPage(page, size);
    }

    @RequestMapping(value = "/movies_per_actor", method = RequestMethod.GET)
    public Page<Report> getMoviesPerActor(@RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "5") int size) {
        return movieService.getMoviesPerActorByPage(page, size);
    }

    @RequestMapping(value = "/movies_per_year", method = RequestMethod.GET)
    public Page<Report> getMoviesPerYear(@RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "5") int size) {
        return movieService.getMoviesPerYearByPage(page, size);
    }

    @RequestMapping(value = "/add", method = RequestMethod.POST)
    public void addMovie(@RequestBody Movie movie) {
        // TODO: add input validation (e.g. null / blank input, etc.)

        // trim whitespaces and reformat inputs for SQL query
        movie.setTitle(movie.getTitle().trim());
        movie.setGenre(movie.getGenre().trim());
        movie.setActor1(movie.getActor1().trim());
        movie.setActor2(movie.getActor2().trim());
        movie.setDirector(movie.getDirector().trim());

        movieService.addMovie(movie);
    }

    @RequestMapping(value = "/update", method = RequestMethod.PUT)
    public void updateMovie(@RequestBody Movie movie) {
        // TODO: add input validation (e.g. null / blank input, etc.)

        // trim whitespaces and reformat inputs for SQL query
        movie.setTitle(movie.getTitle().trim());
        movie.setGenre(movie.getGenre().trim());
        movie.setActor1(movie.getActor1().trim());
        movie.setActor2(movie.getActor2().trim());
        movie.setDirector(movie.getDirector().trim());

        movieService.updateMovie(movie);
    }

    @RequestMapping(value = "/delete", method = RequestMethod.DELETE)
    public void deleteMovie(Movie movie) {
        movieService.deleteMovie(movie);
    }

}
