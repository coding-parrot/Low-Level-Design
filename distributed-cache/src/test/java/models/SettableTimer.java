package models;

public class SettableTimer extends Timer {
    private long time = -1;

    @Override
    public long getCurrentTime() {
        return time == -1 ? System.nanoTime() : time;
    }

    public void setTime(long time) {
        this.time = time;
    }
}
