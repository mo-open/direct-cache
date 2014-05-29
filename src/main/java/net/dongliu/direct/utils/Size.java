package net.dongliu.direct.utils;

import static java.lang.String.format;

public class Size {

    private static final int KILOBYTE_UNIT = 1024;

    public static int Gb(double giga) {
        return (int) giga * KILOBYTE_UNIT * KILOBYTE_UNIT * KILOBYTE_UNIT;
    }

    public static int Mb(double mega) {
        return (int) mega * KILOBYTE_UNIT * KILOBYTE_UNIT;
    }

    public static int Kb(double kilo) {
        return (int) kilo * KILOBYTE_UNIT;
    }

    public static String inKb(long bytes) {
        return format("%(,.1fKb", (double) bytes / KILOBYTE_UNIT);
    }

    public static String inMb(long bytes) {
        return format("%(,.1fMb", (double) bytes / KILOBYTE_UNIT / KILOBYTE_UNIT);
    }

    public static String inGb(long bytes) {
        return format("%(,.1fGb", (double) bytes / KILOBYTE_UNIT / KILOBYTE_UNIT / KILOBYTE_UNIT);
    }

}
