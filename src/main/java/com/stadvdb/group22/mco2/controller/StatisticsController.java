package com.stadvdb.group22.mco2.controller;

import com.stadvdb.group22.mco2.config.ErrorMessageConfig;
import com.stadvdb.group22.mco2.exception.TransactionErrorException;
import com.stadvdb.group22.mco2.model.Movie;
import com.stadvdb.group22.mco2.model.Report;
import com.stadvdb.group22.mco2.service.DistributedDBService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.view.RedirectView;

@Controller
public class StatisticsController {

    @Autowired
    private DistributedDBService distributedDBService;

    @RequestMapping(value = "/statistics", method = RequestMethod.GET)
    public String getStatisticsMenuPage() {
        return "statistics_menu";
    }

    @RequestMapping(value = "/statistics/movies_per_genre", method = RequestMethod.GET)
    public RedirectView redirectGenrePage() {
        return new RedirectView("/statistics/movies_per_genre/p/1");
    }

    @RequestMapping(value = "/statistics/movies_per_director", method = RequestMethod.GET)
    public RedirectView redirectDirectorPage() {
        return new RedirectView("/statistics/movies_per_director/p/1");
    }

    @RequestMapping(value = "/statistics/movies_per_actor", method = RequestMethod.GET)
    public RedirectView redirectActorPage() {
        return new RedirectView("/statistics/movies_per_actor/p/1");
    }

    @RequestMapping(value = "/statistics/movies_per_year", method = RequestMethod.GET)
    public RedirectView redirectYearPage() {
        return new RedirectView("/statistics/movies_per_year/p/1");
    }

    @RequestMapping(value = "/statistics/movies_per_genre/p/{pageNum}", method = RequestMethod.GET)
    public String getMoviesPerGenre(Model model, @PathVariable int pageNum, @RequestParam(defaultValue = "10") int size) {
        try {
            Page<Report> reports = distributedDBService.getMoviesPerGenreByPage(pageNum - 1, size);
            int totalPages = reports.getTotalPages();

            if (pageNum > 0 && pageNum <= totalPages) {
                model.addAttribute("page", reports);
                model.addAttribute("pageNum", pageNum);
                model.addAttribute("pageTitle", "No. of Movies Per Genre:");
                model.addAttribute("statisticsURL", "movies_per_genre");
                return "statistics";
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
        // if database is down
        } catch (Exception e) {
            model.addAttribute("tabTitle", ErrorMessageConfig.TITLE_DB_DOWN);
            model.addAttribute("mainText", ErrorMessageConfig.DB_DOWN);
            model.addAttribute("subText", ErrorMessageConfig.SUB_TEXT);
            return "err_page";
        }
    }

    @RequestMapping(value = "/statistics/movies_per_director/p/{pageNum}", method = RequestMethod.GET)
    public String getMoviesPerDirector(Model model, @PathVariable int pageNum, @RequestParam(defaultValue = "10") int size) {
        try {
            Page<Report> reports = distributedDBService.getMoviesPerDirectorByPage(pageNum - 1, size);
            int totalPages = reports.getTotalPages();

            if (pageNum > 0 && pageNum <= totalPages) {
                model.addAttribute("page", reports);
                model.addAttribute("pageNum", pageNum);
                model.addAttribute("pageTitle", "No. of Movies Per Director:");
                model.addAttribute("statisticsURL", "movies_per_director");
                return "statistics";
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
        // if database is down
        } catch (Exception e) {
            model.addAttribute("tabTitle", ErrorMessageConfig.TITLE_DB_DOWN);
            model.addAttribute("mainText", ErrorMessageConfig.DB_DOWN);
            model.addAttribute("subText", ErrorMessageConfig.SUB_TEXT);
            return "err_page";
        }
    }

    @RequestMapping(value = "/statistics/movies_per_actor/p/{pageNum}", method = RequestMethod.GET)
    public String getMoviesPerActor(Model model, @PathVariable int pageNum, @RequestParam(defaultValue = "10") int size) {
        try {
            Page<Report> reports = distributedDBService.getMoviesPerActorByPage(pageNum - 1, size);
            int totalPages = reports.getTotalPages();

            if (pageNum > 0 && pageNum <= totalPages) {
                model.addAttribute("page", reports);
                model.addAttribute("pageNum", pageNum);
                model.addAttribute("pageTitle", "No. of Movies Per Actor:");
                model.addAttribute("statisticsURL", "movies_per_actor");
                return "statistics";
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
        // if database is down
        } catch (Exception e) {
            model.addAttribute("tabTitle", ErrorMessageConfig.TITLE_DB_DOWN);
            model.addAttribute("mainText", ErrorMessageConfig.DB_DOWN);
            model.addAttribute("subText", ErrorMessageConfig.SUB_TEXT);
            return "err_page";
        }
    }

    @RequestMapping(value = "/statistics/movies_per_year/p/{pageNum}", method = RequestMethod.GET)
    public String getMoviesPerYear(Model model, @PathVariable int pageNum, @RequestParam(defaultValue = "10") int size) {
        try {
            Page<Report> reports = distributedDBService.getMoviesPerYearByPage(pageNum - 1, size);
            int totalPages = reports.getTotalPages();

            if (pageNum > 0 && pageNum <= totalPages) {
                model.addAttribute("page", reports);
                model.addAttribute("pageNum", pageNum);
                model.addAttribute("pageTitle", "No. of Movies Per year:");
                model.addAttribute("statisticsURL", "movies_per_year");
                return "statistics";
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
        // if database is down
        } catch (Exception e) {
            model.addAttribute("tabTitle", ErrorMessageConfig.TITLE_DB_DOWN);
            model.addAttribute("mainText", ErrorMessageConfig.DB_DOWN);
            model.addAttribute("subText", ErrorMessageConfig.SUB_TEXT);
            return "err_page";
        }
    }
}
