package logger;

public class Logger {

    public static void error(String message, Object... args) {
        System.out.println(String.format(message, args));
    }

    public static void error(String message, Throwable t) {
        System.out.println(message);
        t.printStackTrace();
    }

    public static void info(String message, Object... args) {
        System.out.println(String.format(message, args));
    }
}
