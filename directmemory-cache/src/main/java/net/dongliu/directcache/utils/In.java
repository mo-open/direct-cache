package net.dongliu.directcache.utils;

public class In {

    private static final int MILLISECONDS_IN_SECOND = 1000;

    private static final int SECONDS_IN_MINUTE_OR_MINUTES_IN_HOUR = 60;

    private static final int HOUR_IN_DAY = 24;

    private final double measure;

    public In(double measure) {
        this.measure = measure;
    }

    public long seconds() {
        return seconds(measure);
    }

    public long minutes() {
        return minutes(measure);
    }

    public long hours() {
        return hours(measure);
    }

    public long days() {
        return days(measure);
    }

    public static long seconds(double seconds) {
        return (long) seconds * MILLISECONDS_IN_SECOND;
    }

    public static long minutes(double minutes) {
        return seconds(minutes * SECONDS_IN_MINUTE_OR_MINUTES_IN_HOUR);
    }

    public static long hours(double hours) {
        return minutes(hours * SECONDS_IN_MINUTE_OR_MINUTES_IN_HOUR);
    }

    public static long days(double days) {
        return hours(days * HOUR_IN_DAY);
    }

    public static In just(double measure) {
        return new In(measure);
    }

    public static In exactly(double measure) {
        return new In(measure);
    }

    public static In only(double measure) {
        return new In(measure);
    }
}
