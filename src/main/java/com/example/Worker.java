package com.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation;
import edu.stanford.nlp.util.CoreMap;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class Worker {

    // --- Configuration ---
    // QUEUE 2: Manager sends tasks here
    private static final String MANAGER_TO_WORKERS_QUEUE_URL = "YOUR_QUEUE_URL_2"; 
    // QUEUE 3: Worker sends results here
    private static final String WORKERS_TO_MANAGER_QUEUE_URL = "YOUR_QUEUE_URL_3"; 
    // S3 Bucket for output files
    private static final String S3_BUCKET_NAME = "YOUR_S3_BUCKET_NAME";

    // Clients
    private static final SqsClient sqs = SqsClient.create();
    private static final S3Client s3 = S3Client.create();

    public static void main(String[] args) {
        System.out.println("Worker started. Polling for tasks...");

        // Main Loop: Keep running until the instance is terminated by the Manager
        while (true) {
            try {
                // 1. Receive a message from the queue
                List<Message> messages = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(MANAGER_TO_WORKERS_QUEUE_URL)
                        .maxNumberOfMessages(1) // Worker handles one task at a time
                        .waitTimeSeconds(20)    // Long polling
                        .build()).messages();

                for (Message message : messages) {
                    processMessage(message);
                }

            } catch (Exception e) {
                System.err.println("Error in Worker main loop: " + e.getMessage());
                // Sleep briefly to avoid spamming logs if there is a network error
                try { Thread.sleep(5000); } catch (InterruptedException ignored) {}
            }
        }
    }

    private static void processMessage(Message message) {
        String messageBody = message.body();
        System.out.println("Processing message: " + messageBody);

        // Variables to hold parsed data
        String jobKey = "unknown";
        String analysisType = "unknown";
        String url = "unknown";

        try {
            // 2. Parse the message (Format: "jobKey,analysisType,url")
            String[] parts = messageBody.split(",");
            jobKey = parts[0];
            analysisType = parts[1];
            url = parts[2];

            // 3. Download the text from the URL
            String textContent = downloadTextFromUrl(url);

            // 4. Perform the requested analysis
            String analysisResult = performAnalysis(analysisType, textContent);

            // 5. Upload result to S3
            String outputFileName = "output_" + jobKey + "_" + System.currentTimeMillis() + ".txt";
            s3.putObject(PutObjectRequest.builder()
                    .bucket(S3_BUCKET_NAME)
                    .key(outputFileName)
                    .build(),
                    RequestBody.fromString(analysisResult));

            // 6. Send "Done" message to Manager
            // Format expected by Manager: "jobKey,analysisType,originalUrl,s3OutputUrl"
            String s3Url = "s3://" + S3_BUCKET_NAME + "/" + outputFileName;
            String doneMessage = String.format("%s,%s,%s,%s", jobKey, analysisType, url, s3Url);

            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(WORKERS_TO_MANAGER_QUEUE_URL)
                    .messageBody(doneMessage)
                    .build());

            System.out.println("Finished task: " + url);

        } catch (Exception e) {
            System.err.println("Failed to process task: " + messageBody);
            e.printStackTrace();
            
            // Assignment requirement: If exception occurs, notify manager with description
            // We send the error description as the "result"
            sendFailureMessage(jobKey, analysisType, url, "FAILED: " + e.getMessage());
        } finally {
            // 7. Always delete the message so it doesn't get processed again
            sqs.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(MANAGER_TO_WORKERS_QUEUE_URL)
                    .receiptHandle(message.receiptHandle())
                    .build());
        }
    }

    /**
     * Downloads text content from a given URL.
     */
    private static String downloadTextFromUrl(String urlString) throws IOException {
        StringBuilder content = new StringBuilder();
        URL url = new URL(urlString);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
        }
        return content.toString();
    }

    /**
     * Runs the Stanford NLP analysis based on the type.
     */
    private static String performAnalysis(String type, String text) {
        Properties props = new Properties();
        
        // Configure pipeline based on type
        switch (type) {
            case "POS":
                props.setProperty("annotators", "tokenize, ssplit, pos");
                break;
            case "CONSTITUENCY":
                // Parses into a tree structure
                props.setProperty("annotators", "tokenize, ssplit, pos, parse");
                break;
            case "DEPENDENCY":
                // Parses grammatical dependencies
                props.setProperty("annotators", "tokenize, ssplit, pos, depparse");
                break;
            default:
                return "Unknown analysis type: " + type;
        }

        // Disable logging to keep console clean (Optional)
        // props.setProperty("log.stderr", "false");

        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        Annotation document = new Annotation(text);
        pipeline.annotate(document);

        return extractResults(document, type);
    }

    /**
     * Formats the NLP results into a readable string.
     */
    private static String extractResults(Annotation document, String type) {
        StringBuilder result = new StringBuilder();
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);

        for (CoreMap sentence : sentences) {
            if (type.equals("POS")) {
                for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
                    String word = token.get(TextAnnotation.class);
                    String pos = token.get(PartOfSpeechAnnotation.class);
                    result.append(word).append("/").append(pos).append(" ");
                }
                result.append("\n");
            } 
            else if (type.equals("CONSTITUENCY")) {
                Tree tree = sentence.get(TreeAnnotation.class);
                result.append(tree.toString()).append("\n");
            } 
            else if (type.equals("DEPENDENCY")) {
                SemanticGraph dependencies = sentence.get(CollapsedCCProcessedDependenciesAnnotation.class);
                result.append(dependencies.toString(SemanticGraph.OutputFormat.LIST)).append("\n");
            }
        }
        return result.toString();
    }

    /**
     * Sends a failure notification to the manager if a task crashes.
     */
    private static void sendFailureMessage(String jobKey, String op, String inputUrl, String errorMsg) {
        try {
            // Format: "jobKey,analysisType,inputUrl,errorMsg"
            String msg = String.format("%s,%s,%s,%s", jobKey, op, inputUrl, errorMsg);
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(WORKERS_TO_MANAGER_QUEUE_URL)
                    .messageBody(msg)
                    .build());
        } catch (Exception e) {
            System.err.println("Failed to send error report: " + e.getMessage());
        }
    }
}