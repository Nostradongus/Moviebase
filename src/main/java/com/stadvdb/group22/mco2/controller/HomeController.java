package com.stadvdb.group22.mco2.controller;

import com.stadvdb.group22.mco2.config.ErrorMessageConfig;
import com.stadvdb.group22.mco2.exception.ServerMaintenanceException;
import com.stadvdb.group22.mco2.exception.TransactionErrorException;
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

            // if valid page
            if (pageNum >= 0 && pageNum <= totalPages) {
                model.addAttribute("page", movies);
                model.addAttribute("pageNum", pageNum);
                model.addAttribute("movie", new Movie());
                return "index";
                // if invalid page
            } else {
                model.addAttribute("tabTitle", ErrorMessageConfig.TITLE_PAGE_NOT_FOUND);
                model.addAttribute("mainText", ErrorMessageConfig.PAGE_NOT_FOUND);
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
            return "redirect:err_page";
        // if database is down
        } catch (Exception e) {
            model.addAttribute("tabTitle", ErrorMessageConfig.TITLE_DB_DOWN);
            model.addAttribute("mainText", ErrorMessageConfig.DB_DOWN);
            model.addAttribute("subText", ErrorMessageConfig.SUB_TEXT);
            return "err_page";
        }
    }
}
