package de.uksh.medic.etl;

public class UtilMethods {

    @SuppressWarnings("checkstyle:magicnumber")
    public String formatHl7DateTime(String date) {
        return date.substring(0, 4) + "-" + date.substring(4, 6) + "-" + date.substring(6, 8) + "T"
                + date.substring(8, 10) + ":" + date.substring(10, 12) + ":" + date.substring(12, 14);
    }

}
