package com.stadvdb.group22.mco2.controller;

import com.stadvdb.group22.mco2.config.ErrorMessageConfig;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class ErrorController {

    @RequestMapping(value = "/database_down", method= RequestMethod.GET)
    public String getDatabaseDownPage (Model model) {
        model.addAttribute("tabTitle", ErrorMessageConfig.TITLE_DB_DOWN);
        model.addAttribute("mainText", ErrorMessageConfig.DB_DOWN);
        model.addAttribute("subText", ErrorMessageConfig.SUB_TEXT);
        return "err_page";
    }

    @RequestMapping(value = "/error", method = RequestMethod.GET)
    public String getErrorPage (Model model) {
        model.addAttribute("tabTitle", ErrorMessageConfig.TITLE_TRANS_ERROR);
        model.addAttribute("mainText", ErrorMessageConfig.TRANS_ERROR);
        model.addAttribute("subText", ErrorMessageConfig.SUB_TEXT);
        return "err_page";
    }

    @RequestMapping(value = "/maintenance", method = RequestMethod.GET)
    public String getMaintenancePage (Model model) {
        model.addAttribute("tabTitle", ErrorMessageConfig.TITLE_SERVER_MAINTENANCE);
        model.addAttribute("mainText", ErrorMessageConfig.SERVER_MAINTENANCE);
        model.addAttribute("subText", ErrorMessageConfig.SUB_TEXT);
        return "err_page";
    }
}
