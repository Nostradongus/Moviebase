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
        try {
            Page<Movie> movies = distributedDBService.getMoviesByPage(pageNum - 1, size);
            int totalPages = movies.getTotalPages();

            if (pageNum > 0 && pageNum <= totalPages) {
                model.addAttribute("page", movies);
                model.addAttribute("pageNum", pageNum);
                model.addAttribute("movie", new Movie());
                return "index";
            } else {
                return "err_page_not_found";
            }
        } catch (Exception e) {
            return "err_database_down";
        }
    }
}
