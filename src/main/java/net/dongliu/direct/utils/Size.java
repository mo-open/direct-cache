package net.dongliu.direct.utils;

public class Size {

    private static final int KILOBYTE_UNIT = 1024;

    public static int Gb(float giga) {
        return (int) giga * KILOBYTE_UNIT * KILOBYTE_UNIT * KILOBYTE_UNIT;
    }

    public static int Mb(float mega) {
        return (int) mega * KILOBYTE_UNIT * KILOBYTE_UNIT;
    }

    public static int Kb(float kilo) {
        return (int) kilo * KILOBYTE_UNIT;
    }
}
