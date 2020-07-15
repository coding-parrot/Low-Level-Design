import util.Timer;

public class TestTimer extends Timer {
    private long currentTime;

    public TestTimer() {
        this.currentTime = System.nanoTime();
    }

    @Override
    public long getCurrentTime() {
        return currentTime;
    }

    public void setCurrentTime(final long currentTime) {
        this.currentTime = currentTime;
    }
}
