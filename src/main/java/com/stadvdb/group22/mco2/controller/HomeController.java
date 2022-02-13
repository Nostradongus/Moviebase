package com.stadvdb.group22.mco2.controller;

import com.stadvdb.group22.mco2.model.Movie;
import com.stadvdb.group22.mco2.model.Report;
import com.stadvdb.group22.mco2.service.DistributedDBService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.*;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

@Controller
public class HomeController {

    @Autowired
    private DistributedDBService distributedDBService;

    @RequestMapping(value = {"/", ""}, method = RequestMethod.GET)
    public RedirectView home() {
        return new RedirectView("/movies/p/1");
    }

    @RequestMapping(value = {"/movies/p/{pageNum}", ""}, method = RequestMethod.GET)
    public String getHomePage(Model model, @PathVariable int pageNum, @RequestParam(defaultValue = "5") int size) {
        int totalPages = distributedDBService.getMoviesByPage(1, size).getTotalPages();

        if (pageNum > 0 && pageNum < totalPages) {
            model.addAttribute("page", distributedDBService.getMoviesByPage(pageNum - 1, size));
            model.addAttribute("pageNum", pageNum);
            model.addAttribute("movie", new Movie());
            return "index";
        } else {
            return "page_not_found";
        }
    }

    @RequestMapping(value = "/search", method = RequestMethod.POST)
    public String searchMovies(@ModelAttribute Movie movie, @RequestParam(defaultValue = "0") int page,
                                   @RequestParam(defaultValue = "5") int size, Model model) {
        String year = movie.getYear().toString();
        String title = movie.getTitle();
        String genre = movie.getGenre();
        String actor = movie.getActor1();
        String director = movie.getDirector();

        // trim whitespaces and reformat inputs for SQL query
        year = !year.equalsIgnoreCase("") ? year.trim() : null;
        title = !title.equalsIgnoreCase("") ? "'" + title.trim() + "'" : null;
        genre = !genre.equalsIgnoreCase("") ? "'" + genre.trim() + "'" : null;
        actor = !actor.equalsIgnoreCase("") ? "'" + actor.trim() + "'" : null;
        director = !director.equalsIgnoreCase("") ? "'" + director.trim() + "'" : null;

        // create movie object
        Movie movieToInsert = new Movie();
        movieToInsert.setYear(Integer.parseInt(year));
        movieToInsert.setTitle(title);
        movieToInsert.setGenre(genre);
        movieToInsert.setActor1(actor);
        movieToInsert.setDirector(director);

        try {
            model.addAttribute("page", distributedDBService.searchMoviesByPage(movieToInsert, page, size));
            return "search_results";
        } catch (Exception e) {
            // TODO: handle exception for front-end
        }

        return "index";
    }

//    @RequestMapping(value = "/movies_per_genre", method = RequestMethod.GET)
//    public Page<Report> getMoviesPerGenre(@RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "5") int size) {
//        return distributedDBService.getMoviesPerGenreByPage(page, size);
//    }
//
//    @RequestMapping(value = "/movies_per_director", method = RequestMethod.GET)
//    public Page<Report> getMoviesPerDirector(@RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "5") int size) {
//        return distributedDBService.getMoviesPerDirectorByPage(page, size);
//    }
//
//    @RequestMapping(value = "/movies_per_actor", method = RequestMethod.GET)
//    public Page<Report> getMoviesPerActor(@RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "5") int size) {
//        return distributedDBService.getMoviesPerActorByPage(page, size);
//    }
//
//    @RequestMapping(value = "/movies_per_year", method = RequestMethod.GET)
//    public Page<Report> getMoviesPerYear(@RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "5") int size) {
//        return distributedDBService.getMoviesPerYearByPage(page, size);
//    }
//
//    @RequestMapping(value = "/add", method = RequestMethod.POST)
//    public void addMovie(@RequestBody Movie movie) {
//        // TODO: add input validation (e.g. null / blank input, etc.)
//
//        // trim whitespaces and reformat inputs for SQL query
//        movie.setTitle(movie.getTitle().trim());
//        movie.setGenre(movie.getGenre().trim());
//        movie.setActor1(movie.getActor1().trim());
//        movie.setActor2(movie.getActor2().trim());
//        movie.setDirector(movie.getDirector().trim());
//        movie.setUuid(UUID.randomUUID().toString());
//
//        distributedDBService.addMovie(movie);
//    }
//
//    @RequestMapping(value = "/update", method = RequestMethod.PUT)
//    public void updateMovie(@RequestBody Movie movie) {
//        // TODO: add input validation (e.g. null / blank input, etc.)
//
//        // trim whitespaces and reformat inputs for SQL query
//        movie.setTitle(movie.getTitle().trim());
//        movie.setGenre(movie.getGenre().trim());
//        movie.setActor1(movie.getActor1().trim());
//        movie.setActor2(movie.getActor2().trim());
//        movie.setDirector(movie.getDirector().trim());
//
//        distributedDBService.updateMovie(movie);
//    }
//
//    @RequestMapping(value = "/delete", method = RequestMethod.DELETE)
//    public void deleteMovie(Movie movie) {
//        distributedDBService.deleteMovie(movie);
//    }

//    @RequestMapping(value = {"/node1", ""}, method = RequestMethod.GET)
//    public List<Movie> getAllMovies() {
//        return movieService.getAllMovies();
//    }
}
