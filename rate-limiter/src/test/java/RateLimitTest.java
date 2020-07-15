import models.Request;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class RateLimitTest {

    @Test
    public void testDefaultBehaviour() throws Exception {
        final TimeUnit timeUnit = TimeUnit.SECONDS;
        final TestTimer timer = new TestTimer();
        final TimerWheel timerWheel = new TimerWheel(timeUnit, 6, 3, timer);
        timerWheel.addRequest(new Request("1", timer.getCurrentTime(timeUnit))).get();
        timerWheel.addRequest(new Request("2", timer.getCurrentTime(timeUnit))).get();
        timerWheel.addRequest(new Request("3", timer.getCurrentTime(timeUnit))).get();
        Throwable exception = null;
        try {
            timerWheel.addRequest(new Request("4", timer.getCurrentTime(timeUnit))).get();
        } catch (Exception e) {
            exception = e.getCause();
        }
        Assert.assertNotNull(exception);
        Assert.assertEquals("Rate limit exceeded", exception.getMessage());
        tick(timeUnit, timer, timerWheel);
        timerWheel.addRequest(new Request("4", timer.getCurrentTime(timeUnit))).get();
        timerWheel.addRequest(new Request("5", timer.getCurrentTime(timeUnit))).get();
        timerWheel.evict("1").get();
        timerWheel.evict("4").get();
        timerWheel.addRequest(new Request("6", timer.getCurrentTime(timeUnit))).get();
        timerWheel.addRequest(new Request("7", timer.getCurrentTime(timeUnit))).get();
    }

    @Test
    public void testClearing() throws Exception {
        final TimeUnit timeUnit = TimeUnit.SECONDS;
        final TestTimer timer = new TestTimer();
        final int timeOutPeriod = 6;
        final TimerWheel timerWheel = new TimerWheel(timeUnit, timeOutPeriod, 3, timer);
        timerWheel.addRequest(new Request("0", timer.getCurrentTime(timeUnit))).get();
        timerWheel.addRequest(new Request("1", timer.getCurrentTime(timeUnit))).get();
        timerWheel.addRequest(new Request("2", timer.getCurrentTime(timeUnit))).get();

        Throwable exception = null;
        try {
            timerWheel.addRequest(new Request("3", timer.getCurrentTime(timeUnit))).get();
        } catch (Exception e) {
            exception = e.getCause();
        }
        Assert.assertNotNull(exception);
        Assert.assertEquals("Rate limit exceeded", exception.getMessage());

        for (int i = 0; i < timeOutPeriod; i++) {
            tick(timeUnit, timer, timerWheel);
        }
        timerWheel.addRequest(new Request("4", timer.getCurrentTime(timeUnit))).get();
        timerWheel.addRequest(new Request("5", timer.getCurrentTime(timeUnit))).get();
        timerWheel.addRequest(new Request("6", timer.getCurrentTime(timeUnit))).get();

        exception = null;
        try {
            timerWheel.addRequest(new Request("7", timer.getCurrentTime(timeUnit))).get();
        } catch (Exception e) {
            exception = e.getCause();
        }
        Assert.assertNotNull(exception);
        Assert.assertEquals("Rate limit exceeded", exception.getMessage());
    }

    private void tick(TimeUnit timeUnit, TestTimer timer, TimerWheel timerWheel) throws Exception {
        timer.setTime(timer.getCurrentTimeInMillis() + TimeUnit.MILLISECONDS.convert(1, timeUnit));
        timerWheel.flushRequests().get();
    }
}
