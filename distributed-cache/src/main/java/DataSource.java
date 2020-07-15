import java.util.concurrent.CompletionStage;

public interface DataSource<KEY, VALUE> {

    CompletionStage<VALUE> load(KEY key);

    CompletionStage<Void> persist(KEY key, VALUE value, long timestamp);
}
