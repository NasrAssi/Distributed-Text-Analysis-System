package com.example;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class Manager {

    // --- Configuration ---
    final static AWS aws = AWS.getInstance();
    
    // Queues
    private static String LOCAL_APP_TO_MANAGER_QUEUE_URL = aws.getQueueUrl("LocalToManager");
    private static String MANAGER_TO_WORKERS_QUEUE_URL = null;
    private static String WORKERS_TO_MANAGER_QUEUE_URL = null;
    
    // Concurrency
    private static final ExecutorService executor = Executors.newFixedThreadPool(10); // Handles 10 apps at once
    private static final AtomicBoolean shuttingDown = new AtomicBoolean(false);


    // This thread-safe counter tracks all tasks sent to workers
    private static final AtomicInteger outstandingTasks = new AtomicInteger(0);

    // This map will store the results for each job to build the HTML
    // Key = Job ID (e.g., the input file key), Value = List of result lines
    private static final ConcurrentMap<String, JobTracker> jobResults = new ConcurrentHashMap<>();


    public static void main(String[] args) {
        System.out.println("Manager node is running...");

        aws.createSqsQueue("ManagerToWorker");
        aws.createSqsQueue("WorkerToManager");

        System.out.println("created ManagerToWorker queue and WorkerToManager queue");

        MANAGER_TO_WORKERS_QUEUE_URL = aws.getQueueUrl("ManagerToWorker");
        WORKERS_TO_MANAGER_QUEUE_URL = aws.getQueueUrl("WorkerToManager");
        // Start the results collector thread
        Thread resultsThread = new Thread(Manager::resultsCollectorLoop);
        resultsThread.start();

        // Main loop: Polls for messages from Local Applications
        while (!shuttingDown.get()) {
            try {
                List<Message> messages = aws.receiveMessages(LOCAL_APP_TO_MANAGER_QUEUE_URL);

                for (Message message : messages) {
                    String messageBody = message.body();
                    
                    // Handle TERMINATE in the MAIN thread, not the executor
                    if ("TERMINATE".equals(messageBody)) {
                        System.out.println("Termination message received.");
                        shuttingDown.set(true);
                        aws.deleteMessage(LOCAL_APP_TO_MANAGER_QUEUE_URL, message.receiptHandle());
                        break; // Exit the for loop immediately
                    } else {
                        // Only submit regular tasks to the executor
                        executor.submit(() -> processMessage(message));
                    }
                }
            } catch (SqsException e) {
                System.err.println("SQS Error in main loop: " + e.getMessage());
            }
        }

        // --- SHUTDOWN SEQUENCE ---
        System.out.println("Manager is shutting down...");
        
        // 1. Stop accepting new tasks from Local App
        executor.shutdown();
        try {
            executor.awaitTermination(30, TimeUnit.SECONDS); // Wait for parsing to finish
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 2. Wait for all workers to finish
        System.out.println("Waiting for all outstanding worker tasks to complete...");
        while (outstandingTasks.get() > 0) {
            System.out.println(outstandingTasks.get() + " tasks remaining...");
            try {
                Thread.sleep(5000); // Wait 5 seconds and check again
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        System.out.println("All worker tasks completed.");

        // 3. Stop the results collector thread
        try {
            resultsThread.join(); // Wait for the results thread to finish
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 4. Terminate worker instances 
        terminateAllWorkers();

        deleteQueues();

        // 5. Terminate self 
        terminateSelf();
        
    }
    
    /**
     * This function runs in a separate thread for each message.
     */
    private static void processMessage(Message message) {
        try {
            String messageBody = message.body();
            
            // It's a "new task" message
            // Message body is "bucket\tkey\tn\tresponseQueueUrl"
            String[] parts = messageBody.split("\t");
            String bucket = parts[0];
            String key = parts[1];
            int n = Integer.parseInt(parts[2]);
            String responseQueueUrl = parts[3];
            
            handleNewTask(bucket, key, n, responseQueueUrl);

            aws.deleteMessage(LOCAL_APP_TO_MANAGER_QUEUE_URL, message.receiptHandle());

        } catch (Exception e) {
            System.err.println("Failed to process message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Handles the logic for a new input file.
     */
    private static void handleNewTask(String bucket, String key, int n, String responseQueueUrl) {
        System.out.println("Handling new task: " + key);
    

        try {
            // 1. Download the input file from S3
            InputStream s3ObjectStream = aws.getFileStream(bucket, key);
            BufferedReader reader = new BufferedReader(new InputStreamReader(s3ObjectStream));
            int urlCount = 0;
            String line;

            // 2. Create SQS messages for each URL
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t"); // [cite: 14]
                if (parts.length < 2) continue;
                
                String analysisType = parts[0].trim();
                String url = parts[1].trim();

                // The message body for the worker
                // We also include the "jobKey" so the worker can send it back
                // Format: "jobKey,analysisType,url"
                String messageBody = String.format("%s\t%s\t%s", key, analysisType, url);
                
                aws.sendMessageToSQS(MANAGER_TO_WORKERS_QUEUE_URL, messageBody);

                urlCount++;
            }
            reader.close();

            // 3. IMPORTANT: Update the outstanding tasks counter
            outstandingTasks.addAndGet(urlCount);

            jobResults.put(key, new JobTracker(urlCount, responseQueueUrl));

            System.out.println("Created " + urlCount + " tasks for job " + key);

            // 4. Start Worker nodes accordingly
            int requiredWorkers = (int) Math.ceil((double) urlCount / n);
            scaleWorkers(requiredWorkers);

        } catch (Exception e) {
            System.err.println("Failed to handle task " + key + ": " + e.getMessage());
        }
    }

    /**
     * Checks active workers and starts new ones if needed.
     */
    private static void scaleWorkers(int requiredWorkers) {
        try {
            // 1. Find out how many "Worker" instances are "running" or "pending".
            Filter tagFilter = Filter.builder().name("tag:Role").values("WorkerNode").build();
            Filter stateFilter = Filter.builder().name("instance-state-name").values("pending", "running").build();

            DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                    .filters(tagFilter, stateFilter)
                    .build();

            int activeWorkers = 0;
            DescribeInstancesResponse response = aws.describeInstancesRequest(request);
            for (Reservation reservation : response.reservations()) {
                activeWorkers += reservation.instances().size();
            }

            // 2. Calculate how many new workers to start
            int newWorkersToStart = requiredWorkers - activeWorkers;
            // Enforce a maximum of 19 workers total
            newWorkersToStart = Math.min(newWorkersToStart, 18 - activeWorkers);

            if (newWorkersToStart > 0) {
                System.out.println("Starting " + newWorkersToStart + " new worker nodes...");

                // 3. Write a User-Data script for the Worker
                // TODO: Fill in YOUR_S3_BUCKET_NAME
                String workerUserDataScript = "#!/bin/bash\n" +
                    "yum update -y\n" +
                    "yum install java-17-amazon-corretto -y\n" +
                    "aws s3 cp s3://" + aws.bucketName + "/Worker.jar /home/ec2-user/Worker.jar\n" +
                    "java -Xmx800m -cp /home/ec2-user/Worker.jar com.example.Worker > /home/ec2-user/worker.log 2>&1\n";

                // 4. Create a RunInstancesRequest
                aws.createEC2(workerUserDataScript, "WorkerNode", newWorkersToStart);

            } else {
                System.out.println("No new workers needed. Active: " + activeWorkers + ", Required: " + requiredWorkers);
            }
        } catch (Exception e) {
            System.err.println("Error scaling workers: " + e.getMessage());
        }
    }

    /**
 * This loop runs in a separate thread, collecting results from Workers
 * and decrementing the outstandingTasks counter.
 */
    private static void resultsCollectorLoop() {
        System.out.println("ResultsCollector thread started.");
        
        // Loop until shutdown AND all tasks are done
        while (!shuttingDown.get() || outstandingTasks.get() > 0) {
            try {
                List<Message> messages = aws.receiveMessages(WORKERS_TO_MANAGER_QUEUE_URL);
                
                for (Message message : messages) {
                    // Message body from worker: "jobKey:resultLine"
                    String body = message.body();
                    
                    String jobKey;
                    String resultLine;
                    try {
                        // Split "jobKey:rest_of_the_line"
                        int firstDiv = body.indexOf('\t');
                        jobKey = body.substring(0, firstDiv);
                        resultLine = body.substring(firstDiv + 1);
                    } catch (Exception e) {
                        System.err.println("Bad worker message, skipping: " + body);
                        // Decrement global counter anyway to prevent stall
                        outstandingTasks.decrementAndGet(); 
                        aws.deleteMessage(WORKERS_TO_MANAGER_QUEUE_URL, message.receiptHandle());

                        continue;
                    }

                    // Find the job tracker for this job
                    JobTracker job = jobResults.get(jobKey);

                    if (job != null) {
                        job.addResult(resultLine);
                        
                        // Decrement the counter for that specific job
                        int remaining = job.decrementAndGet();

                        // Check if this specific job is now complete
                        if (remaining == 0) {
                            System.out.println("Job " + jobKey + " is complete.");
                            buildAndUploadHtml(jobKey, job.results, job.responseQueueUrl);
                            jobResults.remove(jobKey); // Clean up
                        }
                    } else {
                        System.err.println("Received result for unknown job: " + jobKey);
                    }

                    // Decrement the global counter
                    outstandingTasks.decrementAndGet();
                    
                    // Delete the message
                    aws.deleteMessage(WORKERS_TO_MANAGER_QUEUE_URL, message.receiptHandle());
                }
            } catch (SqsException e) {
                System.err.println("SQS Error in results collector: " + e.getMessage());
            }
        }
        System.out.println("ResultsCollector thread stopping.");
    }

    // Helper method to build and upload the final HTML
    
    private static void buildAndUploadHtml(String jobKey, List<String> results, String responseQueueUrl) {
        System.out.println("Building HTML for " + jobKey + ", replying to " + responseQueueUrl);
        
        // 1. Create a temporary file on the hard drive
        String fileName = "summary_" + jobKey + ".html";
        File file = new File(fileName);

        // Use try-with-resources to automatically close the writer
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            // Write header
            writer.write("<html><head><title>Analysis Results</title></head><body>\n");
            writer.write("<h1>Analysis Results for Job: " + jobKey + "</h1>\n");
            writer.write("<ul>\n");
            
            // Write results line-by-line. 
            // This keeps RAM usage low because we don't store the whole string in memory.
            for (String line : results) {
                try {
                    String[] parts = line.split("\t"); 
                    String op = parts[0];
                    String inputUrl = parts[1];
                    String outputUrl = parts[2];
                    
                    String htmlLine = String.format("<li><strong>%s</strong>: <a href=\"%s\">Input File</a> &nbsp;&rarr;&nbsp; <a href=\"%s\">Output File</a></li>\n", 
                            op, inputUrl, outputUrl);
                    
                    writer.write(htmlLine);
                } catch (Exception e) {
                    // Handle malformed lines gracefully
                    writer.write("<li>" + line + "</li>\n");
                }
            }
            writer.write("</ul></body></html>");
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
            return; // Stop if we can't write the file
        }
        
        // 2. Upload the physical file to S3
        try {
            aws.uploadFileToS3(fileName, file);
            
            System.out.println("Uploaded summary file to S3: " + fileName);

            // 3. Send "Done" message to Local App
            String doneMessage = "done:s3://" + aws.bucketName + "/" + fileName;
            aws.sendMessageToSQS(responseQueueUrl, doneMessage);
            
        } catch (Exception e) {
            System.err.println("Error uploading HTML: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 4. CLEANUP: Delete the temporary file to save disk space!
            if (file.exists()) {
                file.delete();
                System.out.println("Deleted temporary file: " + fileName);
            }
        }
    }

        /**
     * Finds all active EC2 instances tagged as "WorkerNode" and terminates them.
     */
    private static void terminateAllWorkers() {
        System.out.println("Terminating all worker instances...");
        
        // Find instances with "Role=WorkerNode" and are not "terminated"
        Filter tagFilter = Filter.builder().name("tag:Role").values("WorkerNode").build();
        Filter stateFilter = Filter.builder().name("instance-state-name").values("pending", "running").build();
        
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(tagFilter, stateFilter)
                .build();
        
        List<String> workerInstanceIds = new ArrayList<>();
        DescribeInstancesResponse response = aws.describeInstancesRequest(request);

        for (Reservation reservation : response.reservations()) {
            for (Instance instance : reservation.instances()) {
                workerInstanceIds.add(instance.instanceId());
            }
        }
        
        if (!workerInstanceIds.isEmpty()) {
            TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                    .instanceIds(workerInstanceIds)
                    .build();
            aws.terminateInstance(terminateRequest);
            System.out.println("Sent termination request for " + workerInstanceIds.size() + " workers.");
        } else {
            System.out.println("No active workers found to terminate.");
        }
    }

    /**
 * Gets its own instance ID from the AWS Instance Metadata Service
 * and issues a termination request for itself.
 */
    private static void terminateSelf() {
        System.out.println("Terminating Manager (self)...");
        
        String instanceId = null;
        try {
            URL url = new URL("http://169.254.169.254/latest/meta-data/instance-id");
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
                instanceId = reader.readLine().trim();
            }
        } catch (Exception e) {
            System.err.println("[Manager] WARNING: Failed to read Instance ID. Not on EC2?");
        }
            
        if (instanceId != null && !instanceId.isEmpty()) {
            System.out.println("My instance ID is: " + instanceId);
            TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();
            aws.terminateInstance(terminateRequest);
        } else {
            System.err.println("Could not get self instance-id to terminate.");
        }
    }

    private static void deleteQueues() {
        System.out.println("Deleting SQS queues...");
        try {
            aws.deleteQueue(LOCAL_APP_TO_MANAGER_QUEUE_URL);
            aws.deleteQueue(MANAGER_TO_WORKERS_QUEUE_URL);
            aws.deleteQueue(WORKERS_TO_MANAGER_QUEUE_URL);
        } catch (Exception e) {
            System.err.println("Error deleting queues: " + e.getMessage());
        }
    }

    /**
     * Helper class to track the progress of a single job.
     */
    static class JobTracker {
        final List<String> results;
        final AtomicInteger tasksRemaining;
        final String responseQueueUrl;
        
        public JobTracker(int totalTasks, String responseQueueUrl) {
            this.results = Collections.synchronizedList(new ArrayList<>());
            this.tasksRemaining = new AtomicInteger(totalTasks);
            this.responseQueueUrl = responseQueueUrl;
        }
        
        public void addResult(String resultLine) { results.add(resultLine); }
        public int decrementAndGet() { return tasksRemaining.decrementAndGet(); }
    }
}