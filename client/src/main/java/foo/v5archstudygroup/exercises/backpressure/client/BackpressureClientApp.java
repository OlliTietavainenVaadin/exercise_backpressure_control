package foo.v5archstudygroup.exercises.backpressure.client;

import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Main entry point into the client application.
 */
public class BackpressureClientApp {

    // This value should be high enough to cause trouble without any kind of back pressure control. Tweak it if needed
    // to get a suitable amount of trouble to begin with.
    private static final int MESSAGE_COUNT = 600;

    // Once you have gotten everything to work with one worker, increase this to 2 and see if the program still works.
    private static final int WORKERS = 2;

    public static void main(String[] args) throws Exception {
        var client = new RestClient(URI.create("http://localhost:8080"));

        var executorService = Executors.newFixedThreadPool(WORKERS);
        for (int i = 0; i < WORKERS; ++i) {
            var requestGenerator = new ProcessingRequestGenerator(MESSAGE_COUNT);
            var worker = new ProcessingWorker(requestGenerator, client);
            executorService.submit(worker::run);
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS); // The application should stop well before 1 hour
    }
}
