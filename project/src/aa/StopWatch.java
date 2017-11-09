package aa;

/**
 * Created by kevinsteppe on 4/8/15.
 */
public class StopWatch {

    private long start = 0;
    private long end = 0;
    public final int id;

    // constructor
    public StopWatch() {
        id = (int) (Math.random() * 1000);
    }

    // constructor
    public StopWatch(int id) {
        this.id = id;
    }

    // starts the stop watch
    public void start() {
        start = System.currentTimeMillis();
    }

    // Stops the watch, then returns the elapsed time.
    public long stop() {
        end = System.currentTimeMillis();
        return getTime();
    }

    /**
     * If end() has been called, then returns milliseconds elapsed between start
     * and end. If end() has not been called yet, returns milliseconds since
     * start was called (does not stop the watch)
     */
    public long getTime() {
        if (end == 0) {
            return System.currentTimeMillis() - start;
        } else {
            return (end - start);
        }
    }

    // sets start and end to 0
    public void reset() {
        start = end = 0;
    }

}
