package com.stadvdb.group22.mco2.controller;

import com.stadvdb.group22.mco2.config.ErrorMessageConfig;
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
import org.springframework.web.servlet.mvc.support.RedirectAttributes;
import org.springframework.web.servlet.view.RedirectView;

@Controller
public class SearchController {

    @Autowired
    private DistributedDBService distributedDBService;

    private String titleQuery;
    private String genreQuery;
    private String actorQuery;
    private String directorQuery;
    private String yearQuery;

    @RequestMapping(value = "/search", method = RequestMethod.POST)
    public RedirectView redirectSearch (@ModelAttribute Movie movie) {
        this.yearQuery = movie.getYear() == null ? "" : movie.getYear().toString();
        this.titleQuery = movie.getTitle() == null ? "" : movie.getTitle();
        this.genreQuery = movie.getGenre() == null ? "" : movie.getGenre();
        this.actorQuery = movie.getActor1() == null ? "" : movie.getActor1();
        this.directorQuery = movie.getDirector() == null ? "" : movie.getDirector();
        return new RedirectView("/search/p/1");
    }

    @RequestMapping(value = "/search/p/{pageNum}", method = RequestMethod.GET)
    public String searchMovies(@PathVariable int pageNum, @RequestParam(defaultValue = "5") int size, Model model) {

        // trim whitespaces and reformat inputs for SQL query
        if (this.yearQuery != null)
            this.yearQuery = !yearQuery.equalsIgnoreCase("") ? yearQuery.trim() : null;
        if (this.titleQuery != null)
            this.titleQuery = !titleQuery.equalsIgnoreCase("") ? "'" + titleQuery.trim() + "'" : null;
        if (this.genreQuery != null)
            this.genreQuery = !genreQuery.equalsIgnoreCase("") ? "'" + genreQuery.trim() + "'" : null;
        if (this.actorQuery != null)
            this.actorQuery = !actorQuery.equalsIgnoreCase("") ? "'" + actorQuery.trim() + "'" : null;
        if (this.directorQuery != null)
            this.directorQuery = !directorQuery.equalsIgnoreCase("") ? "'" + directorQuery.trim() + "'" : null;

        // create movie object
        Movie movieToInsert = new Movie();
        movieToInsert.setYear(yearQuery != null ? Integer.parseInt(yearQuery) : null);
        movieToInsert.setTitle(this.titleQuery);
        movieToInsert.setGenre(this.genreQuery);
        movieToInsert.setActor1(this.actorQuery);
        movieToInsert.setDirector(this.directorQuery);

        try {
            Page<Movie> movies = distributedDBService.searchMoviesByPage(movieToInsert, pageNum - 1, size);
            int totalPages = movies.getTotalPages();

            if (pageNum >= 0 && pageNum <= totalPages) {
                model.addAttribute("page", movies);
                model.addAttribute("pageNum", pageNum);
                model.addAttribute("movie", new Movie());
                model.addAttribute("hasResults", "TRUE");
                return "search_results";
            } else if (totalPages == 0) {
                model.addAttribute("page", movies);
                model.addAttribute("pageNum", pageNum);
                model.addAttribute("movie", new Movie());
                model.addAttribute("hasResults", "FALSE");
                return "search_results";
            } else {
                model.addAttribute("tabTitle", ErrorMessageConfig.TITLE_PAGE_NOT_FOUND);
                model.addAttribute("mainText", ErrorMessageConfig.PAGE_NOT_FOUND);
                model.addAttribute("subText", ErrorMessageConfig.SUB_TEXT);
                return "err_page";
            }
        } catch (TransactionErrorException e) {
            model.addAttribute("tabTitle", ErrorMessageConfig.TITLE_TRANS_ERROR);
            model.addAttribute("mainText", ErrorMessageConfig.TRANS_ERROR);
            model.addAttribute("subText", ErrorMessageConfig.SUB_TEXT);
            return "err_page";
        } catch (Exception e) {
            e.printStackTrace();
            model.addAttribute("tabTitle", ErrorMessageConfig.TITLE_DB_DOWN);
            model.addAttribute("mainText", ErrorMessageConfig.DB_DOWN);
            model.addAttribute("subText", ErrorMessageConfig.SUB_TEXT);
            return "err_page";
        }
    }
}
