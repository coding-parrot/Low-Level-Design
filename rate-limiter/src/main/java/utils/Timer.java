package utils;

import java.util.concurrent.TimeUnit;

public class Timer {
    public long getCurrentTime(final TimeUnit timeUnit) {
        return timeUnit.convert(getCurrentTimeInMillis(), TimeUnit.MILLISECONDS);
    }

    public long getCurrentTimeInMillis() {
        return System.currentTimeMillis();
    }
}
