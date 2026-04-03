package logger;

public class Logger {

    public static void error(String message, Object... args) {
        System.err.printf((message) + "%n", args);
    }

    public static void error(String message, Throwable t) {
        System.err.println(message);
        t.printStackTrace(System.err);
    }

    public static void info(String message, Object... args) {
        System.out.printf((message) + "%n", args);
    }
}
