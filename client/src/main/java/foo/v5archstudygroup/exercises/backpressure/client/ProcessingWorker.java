package foo.v5archstudygroup.exercises.backpressure.client;

import foo.v5archstudygroup.exercises.backpressure.messages.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;

/**
 * This class is responsible for sending the messages to the server. You are allowed to change this class in any
 * way you like as long as all messages are delivered successfully to the server.
 */
public class ProcessingWorker {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingWorker.class);
    private final ProcessingRequestGenerator requestGenerator;
    private final RestClient client;
    private int sent = 0;
    private int errors = 0;

    private Queue<Messages.ProcessingRequest> failedRequests = new LinkedList<>();

    public ProcessingWorker(ProcessingRequestGenerator requestGenerator, RestClient client) {
        this.requestGenerator = requestGenerator;
        this.client = client;
    }

    public void run() {
        failedRequests.clear();
        while (requestGenerator.hasNext()) {
            Messages.ProcessingRequest message = requestGenerator.next();
            try {
                sent++;
                client.sendToServer(message);
            } catch (Exception ex) {
                LOGGER.error("Error sending request {}: {}", message.getUuid(), ex.getMessage());
                errors++;
                failedRequests.add(message);
            }
        }
        LOGGER.info("Finished sending {} requests with {} errors", sent, errors);
        retryFailedMessages(5);


    }

    private void retryFailedMessages(int retryCount) {
        LOGGER.info("Retrying sending failed messages");
        if (retryCount <= 0) {
            LOGGER.error("Too many retries, but still {} failed", errors);
        }
        while (!failedRequests.isEmpty()) {
            Messages.ProcessingRequest message = failedRequests.poll();
            try {
                client.sendToServer(message);
                errors--;
            } catch (Exception ex) {
                LOGGER.error("Error resending request {}: {}, giving up with this message", message.getUuid(), ex.getMessage());
                errors++;
                failedRequests.add(message);
            }
        }
        if (errors > 0) {
            LOGGER.error("Some messages were left unsent despite retrying, retries left {}", retryCount - 1);
            try {
                Thread.sleep(5000); // could increase this every time
                retryFailedMessages(retryCount - 1);
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted");
            }
        }
    }
}
