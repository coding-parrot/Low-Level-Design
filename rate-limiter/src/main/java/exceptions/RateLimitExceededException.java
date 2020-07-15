package exceptions;

public class RateLimitExceededException extends IllegalStateException {
    public RateLimitExceededException() {
        super("Rate limit exceeded");
    }
}
