package com.example;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
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
import software.amazon.awssdk.services.sqs.model.Message;

public class Worker {
    
    final static AWS aws = AWS.getInstance();

    // --- Configuration ---
    // QUEUE 2: Manager sends tasks here
    private static final String MANAGER_TO_WORKERS_QUEUE_URL = aws.getQueueUrl("ManagerToWorker");
    // QUEUE 3: Worker sends results here
    private static final String WORKERS_TO_MANAGER_QUEUE_URL = aws.getQueueUrl("WorkerToManager");

    public static void main(String[] args) {
        System.out.println("Worker started. Polling for tasks...");

        // Main Loop: Keep running until the instance is terminated by the Manager
        while (true) {
            try {
                // 1. Receive a message from the queue
                List<Message> messages = aws.receiveWorkerMessages(MANAGER_TO_WORKERS_QUEUE_URL);
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
        System.out.println("Processing: " + messageBody);
        
        String jobKey = "unknown";
        String analysisType = "unknown";
        String url = "unknown";
        
        File tempOutputFile = null;

        try {
            // 2. Parse Message (Format: "jobKey,analysisType,url")
            String[] parts = messageBody.split("\t");
            jobKey = parts[0].trim();
            analysisType = parts[1].trim();
            url = parts[2].trim();

            // 3. Initialize Stanford Pipeline (Once per file)
            StanfordCoreNLP pipeline = createPipeline(analysisType);

            // 4. Create Temporary Output File on Disk
            String outputFileName = "output_" + jobKey + "_" + System.currentTimeMillis() + ".txt";
            tempOutputFile = new File(outputFileName);

            // 5. STREAMING LOGIC: Read URL -> Analyze -> Write to File
            // Using streams ensures we never load the whole file into RAM.
            try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(new URL(url).openStream()));
                BufferedWriter writer = new BufferedWriter(new FileWriter(tempOutputFile))
            ) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.trim().isEmpty()) continue;
                    
                    // Analyze ONE line
                    String analyzedLine = analyzeLine(pipeline, line, analysisType);
                    
                    // Write result immediately to disk
                    writer.write(analyzedLine);
                    writer.newLine(); 
                }
            }

            // 6. Upload the physical file to S3 using AWS Helper
            // This helper method uses RequestBody.fromFile, which streams efficiently.
            aws.uploadFileToS3(outputFileName, tempOutputFile);

            // 7. Send "Done" to Manager using AWS Helper
            String s3Url = "https://" + aws.bucketName + ".s3.amazonaws.com/" + outputFileName;
            String doneMessage = String.format("%s\t%s\t%s\t%s", jobKey, analysisType, url, s3Url);

            aws.sendMessageToSQS(WORKERS_TO_MANAGER_QUEUE_URL, doneMessage);

            System.out.println("Finished task: " + url);

        } catch (Exception e) {
            System.err.println("Failed to process task: " + messageBody);
            e.printStackTrace();
            sendFailureMessage(jobKey, analysisType, url, "FAILED: " + e.getMessage());
        } finally {
            // 8. Cleanup: Delete message using AWS Helper
            aws.deleteMessage(MANAGER_TO_WORKERS_QUEUE_URL, message.receiptHandle());
            
            // Delete the temp file to free up disk space
            if (tempOutputFile != null && tempOutputFile.exists()) {
                tempOutputFile.delete(); 
            }
        }
    }

    // --- Helper Methods ---

    private static StanfordCoreNLP createPipeline(String type) {
        Properties props = new Properties();
        switch (type) {
            case "POS":
                props.setProperty("annotators", "tokenize, ssplit, pos");
                break;
            case "CONSTITUENCY":
                props.setProperty("annotators", "tokenize, ssplit, pos, parse");
                break;
            case "DEPENDENCY":
                props.setProperty("annotators", "tokenize, ssplit, pos, depparse");
                break;
            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
        // Disable logging to keep console clean (Optional)
        props.setProperty("log.stderr", "false");
        
        return new StanfordCoreNLP(props);
    }

    private static String analyzeLine(StanfordCoreNLP pipeline, String line, String type) {
        // Run analysis on just this single line
        Annotation document = new Annotation(line);
        pipeline.annotate(document); 

        StringBuilder result = new StringBuilder();
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);

        for (CoreMap sentence : sentences) {
            if (type.equals("POS")) {
                for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
                    String word = token.get(TextAnnotation.class);
                    String pos = token.get(PartOfSpeechAnnotation.class);
                    result.append(word).append("/").append(pos).append(" ");
                }
            } 
            else if (type.equals("CONSTITUENCY")) {
                Tree tree = sentence.get(TreeAnnotation.class);
                result.append(tree.toString());
            } 
            else if (type.equals("DEPENDENCY")) {
                SemanticGraph dependencies = sentence.get(CollapsedCCProcessedDependenciesAnnotation.class);
                result.append(dependencies.toString(SemanticGraph.OutputFormat.LIST));
            }
        }
        return result.toString();
    }

    private static void sendFailureMessage(String jobKey, String op, String inputUrl, String errorMsg) {
        try {
            String msg = String.format("%s\t%s\t%s\t%s", jobKey, op, inputUrl, errorMsg);
            aws.sendMessageToSQS(WORKERS_TO_MANAGER_QUEUE_URL, msg);
        } catch (Exception e) {
            System.err.println("Failed to send error report.");
        }
    }
}