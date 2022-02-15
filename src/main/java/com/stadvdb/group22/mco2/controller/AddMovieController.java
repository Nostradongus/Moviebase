package com.stadvdb.group22.mco2.controller;

import com.stadvdb.group22.mco2.config.ErrorMessageConfig;
import com.stadvdb.group22.mco2.exception.TransactionErrorException;
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

    @RequestMapping(value = "/add_movie", method = RequestMethod.POST)
    public String addMovie(@ModelAttribute Movie movie, Model model) {
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
            return "redirect: add_movie";
        } catch (TransactionErrorException e) {
            model.addAttribute("tabTitle", ErrorMessageConfig.TITLE_TRANS_ERROR);
            model.addAttribute("mainText", ErrorMessageConfig.TRANS_ERROR);
            model.addAttribute("subText", ErrorMessageConfig.SUB_TEXT);
            return "redirect:err_page";
        } catch (Exception e) {
            model.addAttribute("tabTitle", ErrorMessageConfig.TITLE_DB_DOWN);
            model.addAttribute("mainText", ErrorMessageConfig.DB_DOWN);
            model.addAttribute("subText", ErrorMessageConfig.SUB_TEXT);
            return "redirect:err_page";
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
