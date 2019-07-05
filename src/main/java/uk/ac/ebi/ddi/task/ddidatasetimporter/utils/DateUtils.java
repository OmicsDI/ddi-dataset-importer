package uk.ac.ebi.ddi.task.ddidatasetimporter.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {

    private static final String DATE_FORMAT_YYYY = "yyyy/MM/dd";

    public static String getDate() {
        DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_YYYY);
        return dateFormat.format(new Date());
    }

}
