package com.example;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
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

    // ==================== CONFIGURATION ====================
    final static AWS aws = AWS.getInstance();
    
    // Queue URLs
    private static String LOCAL_APP_TO_MANAGER_QUEUE_URL = aws.getQueueUrl("LocalToManager");
    private static String MANAGER_TO_WORKERS_QUEUE_URL = null;
    private static String WORKERS_TO_MANAGER_QUEUE_URL = null;
    
    // Concurrency controls
    private static final ExecutorService executor = Executors.newFixedThreadPool(10);
    private static final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private static final AtomicInteger outstandingTasks = new AtomicInteger(0);

    // Job tracking - uses disk-based storage for scalability
    private static final ConcurrentMap<String, JobTracker> activeJobs = new ConcurrentHashMap<>();


    // ==================== MAIN ====================
    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("       MANAGER NODE STARTING");
        System.out.println("========================================");

        // Initialize queues
        aws.createSqsQueue("ManagerToWorker");
        aws.createSqsQueue("WorkerToManager");
        System.out.println("[INIT] Created ManagerToWorker and WorkerToManager queues");

        MANAGER_TO_WORKERS_QUEUE_URL = aws.getQueueUrl("ManagerToWorker");
        WORKERS_TO_MANAGER_QUEUE_URL = aws.getQueueUrl("WorkerToManager");
        
        // Start the results collector thread
        Thread resultsThread = new Thread(Manager::resultsCollectorLoop, "ResultsCollector");
        resultsThread.start();
        System.out.println("[INIT] ResultsCollector thread started");

        // ==================== MAIN LOOP ====================
        System.out.println("[MAIN] Listening for messages from LocalApps...");
        
        while (!shuttingDown.get()) {
            try {
                List<Message> messages = aws.receiveMessages(LOCAL_APP_TO_MANAGER_QUEUE_URL);

                for (Message message : messages) {
                    String messageBody = message.body();
                    
                    if ("TERMINATE".equals(messageBody)) {
                        System.out.println("[MAIN] TERMINATE signal received!");
                        shuttingDown.set(true);
                        aws.deleteMessage(LOCAL_APP_TO_MANAGER_QUEUE_URL, message.receiptHandle());
                        break;
                    } else {
                        // Process new task in thread pool (parallel handling)
                        executor.submit(() -> processMessage(message));
                    }
                }
            } catch (SqsException e) {
                System.err.println("[MAIN] SQS Error: " + e.getMessage());
            }
        }

        // ==================== SHUTDOWN SEQUENCE ====================
        performShutdown(resultsThread);
    }


    // ==================== MESSAGE PROCESSING ====================
    
    /**
     * Processes a single message from a LocalApp.
     * Runs in the executor thread pool for parallel handling.
     */
    private static void processMessage(Message message) {
        try {
            String messageBody = message.body();
            System.out.println("[TASK] Processing new task message...");
            
            // Parse: "bucket\tkey\tn\tresponseQueueUrl"
            String[] parts = messageBody.split("\t");
            if (parts.length < 4) {
                System.err.println("[TASK] Invalid message format: " + messageBody);
                aws.deleteMessage(LOCAL_APP_TO_MANAGER_QUEUE_URL, message.receiptHandle());
                return;
            }
            
            String bucket = parts[0];
            String key = parts[1];
            int n = Integer.parseInt(parts[2]);
            String responseQueueUrl = parts[3];
            
            handleNewTask(bucket, key, n, responseQueueUrl);

            aws.deleteMessage(LOCAL_APP_TO_MANAGER_QUEUE_URL, message.receiptHandle());

        } catch (Exception e) {
            System.err.println("[TASK] Failed to process message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Handles a new job: downloads input file, creates worker tasks, scales workers.
     */
    private static void handleNewTask(String bucket, String key, int n, String responseQueueUrl) {
        System.out.println("[JOB] Handling new task: " + key);

        int urlCount = 0;
        
        try {
            // 1. Download the input file from S3 (streaming)
            InputStream s3ObjectStream = aws.getFileStream(bucket, key);
            BufferedReader reader = new BufferedReader(new InputStreamReader(s3ObjectStream));
            String line;

            // 2. Send SQS message for each URL
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t"); 
                if (parts.length < 2) continue;
                
                String analysisType = parts[0].trim();
                String url = parts[1].trim();

                // Format: "jobKey\tanalysisType\turl"
                String messageBody = String.format("%s\t%s\t%s", key, analysisType, url);
                aws.sendMessageToSQS(MANAGER_TO_WORKERS_QUEUE_URL, messageBody);
                urlCount++;
            }
            reader.close();

            if (urlCount == 0) {
                System.err.println("[JOB] No valid URLs found in " + key);
                // Notify LocalApp of empty job
                aws.sendMessageToSQS(responseQueueUrl, "done:ERROR - No valid URLs in input file");
                return;
            }

            // 3. Create JobTracker FIRST (can throw IOException)
            JobTracker tracker;
            try {
                tracker = new JobTracker(key, urlCount, responseQueueUrl);
            } catch (IOException e) {
                System.err.println("[JOB] Failed to create temp file for job " + key + ": " + e.getMessage());
                aws.sendMessageToSQS(responseQueueUrl, "done:ERROR - Failed to initialize job tracking");
                return;
            }

            // 4. Only update counters AFTER successful JobTracker creation
            outstandingTasks.addAndGet(urlCount);
            activeJobs.put(key, tracker);

            System.out.println("[JOB] Created " + urlCount + " tasks for job " + key);

            // 5. Scale workers
            int requiredWorkers = (int) Math.ceil((double) urlCount / n);
            scaleWorkers(requiredWorkers);

        } catch (Exception e) {
            System.err.println("[JOB] Failed to handle task " + key + ": " + e.getMessage());
            e.printStackTrace();
        }
    }


    // ==================== WORKER SCALING ====================
    
    /**
     * Checks active workers and starts new ones if needed.
     */
    private static void scaleWorkers(int requiredWorkers) {
        try {
            // Find active workers
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

            // Calculate new workers needed (max 18 to stay under AWS limit of 19 total)
            int newWorkersToStart = requiredWorkers - activeWorkers;
            newWorkersToStart = Math.min(newWorkersToStart, 18 - activeWorkers);
            newWorkersToStart = Math.max(newWorkersToStart, 0); // Ensure non-negative

            if (newWorkersToStart > 0) {
                System.out.println("[SCALE] Starting " + newWorkersToStart + " new workers (Active: " + activeWorkers + ")");

                String workerUserDataScript = "#!/bin/bash\n" +
                    "yum update -y\n" +
                    "yum install java-17-amazon-corretto -y\n" +
                    "aws s3 cp s3://" + aws.bucketName + "/Worker.jar /home/ec2-user/Worker.jar\n" +
                    "java -Xmx800m -cp /home/ec2-user/Worker.jar com.example.Worker > /home/ec2-user/worker.log 2>&1\n";

                aws.createEC2(workerUserDataScript, "WorkerNode", newWorkersToStart);
            } else {
                System.out.println("[SCALE] No new workers needed. Active: " + activeWorkers + ", Required: " + requiredWorkers);
            }
        } catch (Exception e) {
            System.err.println("[SCALE] Error scaling workers: " + e.getMessage());
        }
    }


    // ==================== RESULTS COLLECTOR ====================
    
    /**
     * Runs in a separate thread. Collects results from workers and writes to disk.
     */
    private static void resultsCollectorLoop() {
        System.out.println("[RESULTS] ResultsCollector thread started");
        
        while (!shuttingDown.get() || outstandingTasks.get() > 0) {
            try {
                List<Message> messages = aws.receiveMessages(WORKERS_TO_MANAGER_QUEUE_URL);
                
                for (Message message : messages) {
                    processWorkerResult(message);
                }
            } catch (SqsException e) {
                System.err.println("[RESULTS] SQS Error: " + e.getMessage());
            }
        }
        
        System.out.println("[RESULTS] ResultsCollector thread stopping");
    }

    /**
     * Processes a single result message from a worker.
     */
    private static void processWorkerResult(Message message) {
        String body = message.body();
        
        // Parse: "jobKey\tanalysisType\tinputUrl\toutputUrl"
        String jobKey;
        String resultLine;
        
        try {
            int firstTab = body.indexOf('\t');
            if (firstTab == -1) throw new IllegalArgumentException("No tab found");
            
            jobKey = body.substring(0, firstTab);
            resultLine = body.substring(firstTab + 1);
        } catch (Exception e) {
            System.err.println("[RESULTS] Malformed worker message: " + body);
            outstandingTasks.decrementAndGet();
            aws.deleteMessage(WORKERS_TO_MANAGER_QUEUE_URL, message.receiptHandle());
            return;
        }

        // Find the job tracker
        JobTracker job = activeJobs.get(jobKey);

        if (job != null) {
            // Write result to disk immediately (scalable!)
            job.addResult(resultLine);
            
            int remaining = job.decrementAndGet();
            System.out.println("[RESULTS] Job " + jobKey + ": " + remaining + " tasks remaining");

            // Check if job is complete
            if (remaining == 0) {
                System.out.println("[RESULTS] Job " + jobKey + " COMPLETE!");
                job.close();
                buildAndUploadHtml(jobKey, job);
                activeJobs.remove(jobKey);
            }
        } else {
            System.err.println("[RESULTS] Result for unknown job: " + jobKey);
        }

        // Update global counter and delete message
        outstandingTasks.decrementAndGet();
        aws.deleteMessage(WORKERS_TO_MANAGER_QUEUE_URL, message.receiptHandle());
    }


    // ==================== HTML GENERATION ====================
    
    /**
     * Builds HTML from partial results file and uploads to S3.
     */
    private static void buildAndUploadHtml(String jobKey, JobTracker job) {
        System.out.println("[HTML] Building HTML for job " + jobKey);
        
        String htmlFileName = "summary_" + jobKey + ".html";
        File htmlFile = new File(htmlFileName);

        // Read partial results from disk, write HTML
        try (
            BufferedReader reader = new BufferedReader(new FileReader(job.getTempFile()));
            BufferedWriter writer = new BufferedWriter(new FileWriter(htmlFile))
        ) {
            // Write HTML header
            writer.write("<!DOCTYPE html>\n");
            writer.write("<html>\n<head>\n");
            writer.write("  <title>Analysis Results - " + jobKey + "</title>\n");
            writer.write("  <style>\n");
            writer.write("    body { font-family: Arial, sans-serif; margin: 40px; }\n");
            writer.write("    h1 { color: #333; }\n");
            writer.write("    ul { list-style-type: none; padding: 0; }\n");
            writer.write("    li { padding: 10px; margin: 5px 0; background: #f5f5f5; border-radius: 5px; }\n");
            writer.write("    a { color: #0066cc; }\n");
            writer.write("    .type { font-weight: bold; color: #333; }\n");
            writer.write("    .error { background: #ffe0e0; }\n");
            writer.write("  </style>\n");
            writer.write("</head>\n<body>\n");
            writer.write("<h1>Analysis Results</h1>\n");
            writer.write("<p>Job ID: " + jobKey + "</p>\n");
            writer.write("<ul>\n");
            
            // Read each result line and format as HTML
            String line;
            while ((line = reader.readLine()) != null) {
                try {
                    String[] parts = line.split("\t");
                    if (parts.length >= 3) {
                        String op = parts[0];
                        String inputUrl = parts[1];
                        String outputUrl = parts[2];
                        
                        // Check if it's an error result
                        if (outputUrl.startsWith("FAILED:") || outputUrl.startsWith("ERROR:")) {
                            writer.write(String.format(
                                "<li class=\"error\"><span class=\"type\">%s</span>: " +
                                "<a href=\"%s\">Input File</a> &nbsp;&rarr;&nbsp; %s</li>\n",
                                op, inputUrl, outputUrl
                            ));
                        } else {
                            writer.write(String.format(
                                "<li><span class=\"type\">%s</span>: " +
                                "<a href=\"%s\">Input File</a> &nbsp;&rarr;&nbsp; " +
                                "<a href=\"%s\">Output File</a></li>\n",
                                op, inputUrl, outputUrl
                            ));
                        }
                    } else {
                        // Malformed line - just display as-is
                        writer.write("<li class=\"error\">" + escapeHtml(line) + "</li>\n");
                    }
                } catch (Exception e) {
                    writer.write("<li class=\"error\">Error parsing result: " + escapeHtml(line) + "</li>\n");
                }
            }
            
            // Write HTML footer
            writer.write("</ul>\n");
            writer.write("<p><em>Generated by DSP Text Analysis System</em></p>\n");
            writer.write("</body>\n</html>");
            
        } catch (IOException e) {
            System.err.println("[HTML] Error building HTML: " + e.getMessage());
            // Try to notify LocalApp of failure
            try {
                aws.sendMessageToSQS(job.getResponseQueueUrl(), "done:ERROR - Failed to build HTML");
            } catch (Exception ex) {
                System.err.println("[HTML] Could not notify LocalApp of failure");
            }
            return;
        }
        
        // Upload HTML to S3
        try {
            aws.uploadFileToS3(htmlFileName, htmlFile);
            System.out.println("[HTML] Uploaded: " + htmlFileName);

            // Notify LocalApp
            String doneMessage = "done:s3://" + aws.bucketName + "/" + htmlFileName;
            aws.sendMessageToSQS(job.getResponseQueueUrl(), doneMessage);
            System.out.println("[HTML] Notified LocalApp at " + job.getResponseQueueUrl());
            
        } catch (Exception e) {
            System.err.println("[HTML] Error uploading: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Cleanup temp files
            if (htmlFile.exists()) {
                htmlFile.delete();
                System.out.println("[HTML] Deleted temp file: " + htmlFileName);
            }
            File tempFile = job.getTempFile();
            if (tempFile.exists()) {
                tempFile.delete();
                System.out.println("[HTML] Deleted temp file: " + tempFile.getName());
            }
        }
    }

    /**
     * Escapes HTML special characters to prevent XSS.
     */
    private static String escapeHtml(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;")
                   .replace("\"", "&quot;");
    }


    // ==================== SHUTDOWN ====================
    
    /**
     * Performs graceful shutdown of the Manager.
     */
    private static void performShutdown(Thread resultsThread) {
        System.out.println("========================================");
        System.out.println("       MANAGER SHUTTING DOWN");
        System.out.println("========================================");
        
        // 1. Stop accepting new tasks
        System.out.println("[SHUTDOWN] Stopping executor...");
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // 2. Wait for all outstanding tasks
        System.out.println("[SHUTDOWN] Waiting for outstanding tasks...");
        while (outstandingTasks.get() > 0) {
            System.out.println("[SHUTDOWN] " + outstandingTasks.get() + " tasks remaining...");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        System.out.println("[SHUTDOWN] All tasks completed!");

        // 3. Wait for results collector thread
        System.out.println("[SHUTDOWN] Stopping ResultsCollector...");
        try {
            resultsThread.join(10000); // Wait max 10 seconds
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 4. Close any remaining job trackers (in case of unexpected shutdown)
        for (JobTracker job : activeJobs.values()) {
            job.close();
            File tempFile = job.getTempFile();
            if (tempFile.exists()) {
                tempFile.delete();
            }
        }

        // 5. Terminate workers
        terminateAllWorkers();

        // 6. Delete queues
        deleteQueues();

        // 7. Terminate self
        terminateSelf();
        
        System.out.println("========================================");
        System.out.println("       MANAGER SHUTDOWN COMPLETE");
        System.out.println("========================================");
    }

    /**
     * Terminates all worker EC2 instances.
     */
    private static void terminateAllWorkers() {
        System.out.println("[SHUTDOWN] Terminating all workers...");
        
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
            System.out.println("[SHUTDOWN] Terminated " + workerInstanceIds.size() + " workers");
        } else {
            System.out.println("[SHUTDOWN] No active workers to terminate");
        }
    }

    /**
     * Gets own instance ID and terminates self.
     */
    private static void terminateSelf() {
        System.out.println("[SHUTDOWN] Terminating self...");
        
        String instanceId = null;
        try {
            URL url = new URL("http://169.254.169.254/latest/meta-data/instance-id");
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
                instanceId = reader.readLine().trim();
            }
        } catch (Exception e) {
            System.err.println("[SHUTDOWN] Could not get instance ID (not on EC2?)");
            return;
        }
            
        if (instanceId != null && !instanceId.isEmpty()) {
            System.out.println("[SHUTDOWN] My instance ID: " + instanceId);
            TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();
            aws.terminateInstance(terminateRequest);
        }
    }

    /**
     * Deletes all SQS queues used by the system.
     */
    private static void deleteQueues() {
        System.out.println("[SHUTDOWN] Deleting SQS queues...");
        try {
            aws.deleteQueue(LOCAL_APP_TO_MANAGER_QUEUE_URL);
            aws.deleteQueue(MANAGER_TO_WORKERS_QUEUE_URL);
            aws.deleteQueue(WORKERS_TO_MANAGER_QUEUE_URL);
            System.out.println("[SHUTDOWN] Queues deleted");
        } catch (Exception e) {
            System.err.println("[SHUTDOWN] Error deleting queues: " + e.getMessage());
        }
    }


    // ==================== JOB TRACKER (DISK-BASED) ====================
    
    /**
     * Tracks a single job's progress and stores results on disk.
     * 
     * SCALABILITY: Results are written to disk immediately instead of being
     * stored in memory. This allows handling jobs with millions of URLs
     * without running out of RAM.
     * 
     * PERSISTENCE: If the Manager crashes, partial results are saved on disk.
     */
    static class JobTracker {
        private final File tempFile;
        private final BufferedWriter writer;
        private final AtomicInteger tasksRemaining;
        private final String responseQueueUrl;
        private final Object writeLock = new Object(); // For thread safety
        
        /**
         * Creates a new JobTracker with a temporary file for results.
         * 
         * @param jobKey Unique identifier for this job
         * @param totalTasks Total number of URLs to process
         * @param responseQueueUrl SQS queue URL to send completion notification
         * @throws IOException If temp file cannot be created
         */
        public JobTracker(String jobKey, int totalTasks, String responseQueueUrl) throws IOException {
            this.tasksRemaining = new AtomicInteger(totalTasks);
            this.responseQueueUrl = responseQueueUrl;
            
            // Create temp file for storing results
            // Using jobKey in filename for debugging purposes
            this.tempFile = new File("partial_results_" + sanitizeFileName(jobKey) + ".txt");
            this.writer = new BufferedWriter(new FileWriter(tempFile, false)); // false = overwrite
            
            System.out.println("[JobTracker] Created temp file: " + tempFile.getName());
        }
        
        /**
         * Adds a result line to the temp file.
         * Thread-safe - can be called from multiple threads.
         */
        public void addResult(String resultLine) {
            synchronized (writeLock) {
                try {
                    writer.write(resultLine);
                    writer.newLine();
                    writer.flush(); // Ensure data is persisted immediately
                } catch (IOException e) {
                    System.err.println("[JobTracker] Disk write error: " + e.getMessage());
                }
            }
        }
        
        /**
         * Decrements the remaining task counter.
         * @return Number of tasks still remaining
         */
        public int decrementAndGet() {
            return tasksRemaining.decrementAndGet();
        }
        
        /**
         * Closes the file writer. Must be called before reading the file.
         */
        public synchronized void close() {
            synchronized (writeLock) {
                try {
                    writer.close();
                } catch (IOException e) {
                    System.err.println("[JobTracker] Error closing file: " + e.getMessage());
                }
            }
        }
        
        /**
         * @return The temporary file containing results
         */
        public File getTempFile() {
            return tempFile;
        }
        
        /**
         * @return The response queue URL for notifying LocalApp
         */
        public String getResponseQueueUrl() {
            return responseQueueUrl;
        }
        
        /**
         * Sanitizes a string to be safe for use in a filename.
         */
        private static String sanitizeFileName(String name) {
            return name.replaceAll("[^a-zA-Z0-9._-]", "_");
        }
    }
}