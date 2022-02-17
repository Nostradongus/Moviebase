package com.stadvdb.group22.mco2.controller;

import com.stadvdb.group22.mco2.config.ErrorMessageConfig;
import com.stadvdb.group22.mco2.exception.ServerMaintenanceException;
import com.stadvdb.group22.mco2.exception.TransactionErrorException;
import com.stadvdb.group22.mco2.model.Movie;
import com.stadvdb.group22.mco2.model.Report;
import com.stadvdb.group22.mco2.service.DistributedDBService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
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
        this.yearQuery = movie.getYear() == null ? null : movie.getYear().toString();
        this.titleQuery = movie.getTitle().equalsIgnoreCase("") ? null : "'" + movie.getTitle() + "'";
        this.genreQuery = movie.getGenre().equalsIgnoreCase("") ? null : "'" + movie.getGenre() + "'";
        this.actorQuery = movie.getActor1().equalsIgnoreCase("") ? null : "'" + movie.getActor1() + "'";
        this.directorQuery = movie.getDirector().equalsIgnoreCase("") ? null : "'" + movie.getDirector() + "'";
        return new RedirectView("/search/p/1");
    }

    @RequestMapping(value = "/search/p/{pageNum}", method = RequestMethod.GET)
    public String searchMovies(@PathVariable int pageNum, @RequestParam(defaultValue = "5") int size, Model model) {
        // create movie object
        Movie movieToInsert = new Movie();
        movieToInsert.setYear(yearQuery != null ? Integer.parseInt(yearQuery) : null);
        movieToInsert.setTitle(titleQuery);
        movieToInsert.setGenre(genreQuery);
        movieToInsert.setActor1(actorQuery);
        movieToInsert.setDirector(directorQuery);

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
        // if there is an error with the transaction
        } catch (TransactionErrorException e) {
            model.addAttribute("tabTitle", ErrorMessageConfig.TITLE_TRANS_ERROR);
            model.addAttribute("mainText", ErrorMessageConfig.TRANS_ERROR);
            model.addAttribute("subText", ErrorMessageConfig.SUB_TEXT);
            return "err_page";
        // if server is in maintenance
        } catch (ServerMaintenanceException e) {
            model.addAttribute("tabTitle", ErrorMessageConfig.TITLE_SERVER_MAINTENANCE);
            model.addAttribute("mainText", ErrorMessageConfig.SERVER_MAINTENANCE);
            model.addAttribute("subText", ErrorMessageConfig.SUB_TEXT);
            return "redirect:err_page";
        // if database is down
        } catch (Exception e) {
            e.printStackTrace();
            model.addAttribute("tabTitle", ErrorMessageConfig.TITLE_DB_DOWN);
            model.addAttribute("mainText", ErrorMessageConfig.DB_DOWN);
            model.addAttribute("subText", ErrorMessageConfig.SUB_TEXT);
            return "err_page";
        }
    }
}
