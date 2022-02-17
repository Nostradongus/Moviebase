package com.stadvdb.group22.mco2.controller;

import com.stadvdb.group22.mco2.config.ErrorMessageConfig;
import com.stadvdb.group22.mco2.exception.ServerMaintenanceException;
import com.stadvdb.group22.mco2.exception.TransactionErrorException;
import com.stadvdb.group22.mco2.model.Movie;
import com.stadvdb.group22.mco2.service.DistributedDBService;
import org.apache.catalina.Server;
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

                model.addAttribute("tabTitle", ErrorMessageConfig.TITLE_MOVIE_NOT_FOUND);
                model.addAttribute("mainText", ErrorMessageConfig.MOVIE_NOT_FOUND);
                model.addAttribute("subText", ErrorMessageConfig.SUB_TEXT);
                return "err_page";
            }
        // if error occurred during query
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
            return "err_page";
        // if database is down
        } catch (Exception e) {
            this.movieUUID = "";
            this.movieYear = -1;

            model.addAttribute("tabTitle", ErrorMessageConfig.TITLE_DB_DOWN);
            model.addAttribute("mainText", ErrorMessageConfig.DB_DOWN);
            model.addAttribute("subText", ErrorMessageConfig.SUB_TEXT);
            return "err_page";
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

        try {
            distributedDBService.updateMovie(movie);
            return new RedirectView ("/");
        // if there is an error with the transaction
        } catch (TransactionErrorException e) {
            return new RedirectView ("/error");
        // if server is in maintenance
        } catch (ServerMaintenanceException e) {
            return new RedirectView ("/maintenance");
        // if database is down
        } catch (Exception e) {
            return new RedirectView ("/database_down");
        }
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
