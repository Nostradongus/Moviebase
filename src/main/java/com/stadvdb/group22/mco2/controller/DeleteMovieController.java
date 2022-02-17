package com.stadvdb.group22.mco2.controller;

import com.stadvdb.group22.mco2.config.ErrorMessageConfig;
import com.stadvdb.group22.mco2.exception.ServerMaintenanceException;
import com.stadvdb.group22.mco2.exception.TransactionErrorException;
import com.stadvdb.group22.mco2.model.Movie;
import com.stadvdb.group22.mco2.service.DistributedDBService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.view.RedirectView;

@Controller
public class DeleteMovieController {

    @Autowired
    private DistributedDBService distributedDBService;

    @RequestMapping(value = "/delete/{movieYear}/{movieUUID}/", method = RequestMethod.GET)
    public RedirectView deleteMovie(@PathVariable String movieUUID, @PathVariable int movieYear) {
        Movie movie = new Movie ();
        movie.setUuid(movieUUID);
        movie.setYear(movieYear);
        try {
            distributedDBService.deleteMovie(movie);
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
}
