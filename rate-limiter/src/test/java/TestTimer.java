import utils.Timer;

public class TestTimer extends Timer {
    private long currentTime = System.currentTimeMillis();

    @Override
    public long getCurrentTimeInMillis() {
        return currentTime;
    }

    public void setTime(final long currentTime) {
        this.currentTime = currentTime;
    }
}
