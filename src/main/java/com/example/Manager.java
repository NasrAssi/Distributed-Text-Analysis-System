package com.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class Manager {

    // --- Configuration ---
    // Queues
    private static final String LOCAL_APP_TO_MANAGER_QUEUE_URL = "YOUR_QUEUE_URL_1";
    private static final String MANAGER_TO_WORKERS_QUEUE_URL = "YOUR_QUEUE_URL_2";
    private static final String WORKERS_TO_MANAGER_QUEUE_URL = "YOUR_QUEUE_URL_3";
    private static final String MANAGER_TO_LOCAL_APP_QUEUE_URL = "YOUR_QUEUE_URL_4";
    // Clients
    private static final SqsClient sqs = SqsClient.create();
    private static final S3Client s3 = S3Client.create();
    private static final Ec2Client ec2 = Ec2Client.create();
    
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

        // Start the results collector thread
        Thread resultsThread = new Thread(Manager::resultsCollectorLoop);
        resultsThread.start();

        // Main loop: Polls for messages from Local Applications
        while (!shuttingDown.get()) {
            try {
                ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                        .queueUrl(LOCAL_APP_TO_MANAGER_QUEUE_URL)
                        .maxNumberOfMessages(5)
                        .waitTimeSeconds(20) // Long polling
                        .build();

                List<Message> messages = sqs.receiveMessage(receiveRequest).messages();

                for (Message message : messages) {
                    // Give the task to a thread to run in parallel
                    executor.submit(() -> processMessage(message));
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
        terminateAllWorkers(ec2);

        // 5. Terminate self 
        terminateSelf(ec2);
        
    }
    
    /**
     * This function runs in a separate thread for each message.
     */
    private static void processMessage(Message message) {
        try {
            String messageBody = message.body();
            
            // Check if it's a "terminate" message
            if ("TERMINATE".equals(messageBody)) {
                System.out.println("Termination message received.");
                shuttingDown.set(true); // [cite: 62]
            
            } else {
                // It's a "new task" message [cite: 51]
                // Message body is "bucket:key:n"
                String[] parts = messageBody.split(",");
                String bucket = parts[0];
                String key = parts[1];
                int n = Integer.parseInt(parts[2]); // Max files per worker [cite: 11]

                handleNewTask(bucket, key, n);
            }

            // Acknowledge and delete the message from the queue
            sqs.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(LOCAL_APP_TO_MANAGER_QUEUE_URL)
                    .receiptHandle(message.receiptHandle())
                    .build());

        } catch (Exception e) {
            System.err.println("Failed to process message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Handles the logic for a new input file.
     */
    private static void handleNewTask(String bucket, String key, int n) {
        System.out.println("Handling new task: " + key);
    

        try {
            // 1. Download the input file from S3
            InputStream s3ObjectStream = s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build());
            BufferedReader reader = new BufferedReader(new InputStreamReader(s3ObjectStream));
            int urlCount = 0;
            String line;

            // 2. Create SQS messages for each URL
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t"); // [cite: 14]
                if (parts.length < 2) continue;
                
                String analysisType = parts[0];
                String url = parts[1];

                // The message body for the worker
                // We also include the "jobKey" so the worker can send it back
                // Format: "jobKey,analysisType,url"
                String messageBody = String.format("%s,%s,%s", key, analysisType, url);
                
                sqs.sendMessage(SendMessageRequest.builder()
                        .queueUrl(MANAGER_TO_WORKERS_QUEUE_URL)
                        .messageBody(messageBody)
                        .build());
                urlCount++;
            }
            reader.close();

            // 3. IMPORTANT: Update the outstanding tasks counter
            outstandingTasks.addAndGet(urlCount);

            jobResults.put(key, new JobTracker(urlCount));

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
            DescribeInstancesResponse response = ec2.describeInstances(request);
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
                    "aws s3 cp s3://YOUR_S3_BUCKET_NAME/Worker.jar /home/ec2-user/Worker.jar\n" +
                    "java -jar /home/ec2-user/Worker.jar\n";
                String workerUserDataBase64 = java.util.Base64.getEncoder().encodeToString(workerUserDataScript.getBytes());

                // 4. Create a RunInstancesRequest
                // TODO: Fill in YOUR_AMI_ID, YOUR_KEY_NAME, YOUR_SECURITY_GROUP_ID, and YOUR_IAM_ROLE_NAME
                var runRequest = RunInstancesRequest.builder()
                    .imageId("YOUR_AMI_ID") // e.g., ami-076515f20540e6e0b
                    .instanceType(software.amazon.awssdk.services.ec2.model.InstanceType.T2_MICRO)
                    .maxCount(newWorkersToStart)
                    .minCount(newWorkersToStart)
                    .userData(workerUserDataBase64)
                    .keyName("YOUR_KEY_NAME") // Your .pem key
                    .securityGroupIds("YOUR_SECURITY_GROUP_ID")
                    .iamInstanceProfile(software.amazon.awssdk.services.ec2.model.IamInstanceProfileSpecification.builder()
                            .name("YOUR_IAM_ROLE_NAME") // The IAM Role with S3/SQS/EC2 access
                            .build())
                    .build();

                RunInstancesResponse runResponse = ec2.runInstances(runRequest);

                // 5. Tag the new instances with Role=WorkerNode
                List<String> newInstanceIds = runResponse.instances().stream()
                        .map(Instance::instanceId)
                        .collect(java.util.stream.Collectors.toList());
                
                Tag tag = Tag.builder().key("Role").value("WorkerNode").build();
                CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                        .resources(newInstanceIds)
                        .tags(tag)
                        .build();
                ec2.createTags(tagRequest);

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
                List<Message> messages = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(WORKERS_TO_MANAGER_QUEUE_URL)
                        .maxNumberOfMessages(10)
                        .waitTimeSeconds(5)
                        .build()).messages();

                for (Message message : messages) {
                    // Message body from worker: "jobKey:resultLine"
                    String body = message.body();
                    
                    String jobKey;
                    String resultLine;
                    try {
                        // Split "jobKey:rest_of_the_line"
                        int firstComma = body.indexOf(',');
                        jobKey = body.substring(0, firstComma);
                        resultLine = body.substring(firstComma + 1);
                    } catch (Exception e) {
                        System.err.println("Bad worker message, skipping: " + body);
                        // Decrement global counter anyway to prevent stall
                        outstandingTasks.decrementAndGet(); 

                        sqs.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(WORKERS_TO_MANAGER_QUEUE_URL)
                            .receiptHandle(message.receiptHandle())
                            .build());

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
                            buildAndUploadHtml(jobKey, job.results);
                            jobResults.remove(jobKey); // Clean up
                        }
                    } else {
                        System.err.println("Received result for unknown job: " + jobKey);
                    }

                    // Decrement the global counter
                    outstandingTasks.decrementAndGet();
                    
                    // Delete the message
                    sqs.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(WORKERS_TO_MANAGER_QUEUE_URL)
                        .receiptHandle(message.receiptHandle())
                        .build());
                }
            } catch (SqsException e) {
                System.err.println("SQS Error in results collector: " + e.getMessage());
            }
        }
        System.out.println("ResultsCollector thread stopping.");
    }

    // Helper method to build and upload the final HTML
    private static void buildAndUploadHtml(String jobKey, List<String> results) {
        System.out.println("Building HTML for " + jobKey);
        
        // 1. Build an HTML string from the 'results' list
        StringBuilder html = new StringBuilder();
        html.append("<html><head><title>Analysis Results</title></head><body>\n");
        html.append("<h1>Analysis Results for Job: ").append(jobKey).append("</h1>\n");
        html.append("<ul>\n");
        
        for (String line : results) {
            // Expected format received from Worker: "analysisType,inputUrl,s3OutputUrl"
            // We parse this to create clickable links
            try {
                String[] parts = line.split(","); 
                String op = parts[0];
                String inputUrl = parts[1];
                String outputUrl = parts[2];
                
                html.append("<li>")
                    .append("<strong>").append(op).append("</strong>: ")
                    .append("<a href=\"").append(inputUrl).append("\">Input File</a> ")
                    .append("&nbsp;&rarr;&nbsp; ")
                    .append("<a href=\"").append(outputUrl).append("\">Output File</a>")
                    .append("</li>\n");
            } catch (Exception e) {
                // If the line is an error message or malformed
                html.append("<li>").append(line).append("</li>\n");
            }
        }
        html.append("</ul></body></html>");
        
        // 2. Upload this HTML string to S3
        String outputKey = "summary_" + jobKey + ".html";
        
        try {
            s3.putObject(software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                    .bucket("YOUR_S3_BUCKET_NAME") // TODO: Ensure this matches your real bucket
                    .key(outputKey)
                    .build(),
                    software.amazon.awssdk.core.sync.RequestBody.fromString(html.toString()));
            
            System.out.println("Uploaded summary file to S3: " + outputKey);

            // 3. Send a "done" message to the Local Application
            // We send the full S3 path so the Local App can download it
            String bucketName = "YOUR_S3_BUCKET_NAME"; // TODO: Ensure this matches
            String doneMessage = "done:s3://" + bucketName + "/" + outputKey;
            
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(MANAGER_TO_LOCAL_APP_QUEUE_URL) // Sending response back to the App queue
                    .messageBody(doneMessage)
                    .build());
            
        } catch (Exception e) {
            System.err.println("Error uploading HTML or notifying user: " + e.getMessage());
            e.printStackTrace();
        }
    }


        /**
     * Finds all active EC2 instances tagged as "WorkerNode" and terminates them.
     */
    private static void terminateAllWorkers(Ec2Client ec2) {
        System.out.println("Terminating all worker instances...");
        
        // Find instances with "Role=WorkerNode" and are not "terminated"
        Filter tagFilter = Filter.builder().name("tag:Role").values("WorkerNode").build();
        Filter stateFilter = Filter.builder().name("instance-state-name").values("pending", "running").build();
        
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(tagFilter, stateFilter)
                .build();
        
        List<String> workerInstanceIds = new ArrayList<>();
        DescribeInstancesResponse response = ec2.describeInstances(request);
        
        for (Reservation reservation : response.reservations()) {
            for (Instance instance : reservation.instances()) {
                workerInstanceIds.add(instance.instanceId());
            }
        }
        
        if (!workerInstanceIds.isEmpty()) {
            TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                    .instanceIds(workerInstanceIds)
                    .build();
            ec2.terminateInstances(terminateRequest);
            System.out.println("Sent termination request for " + workerInstanceIds.size() + " workers.");
        } else {
            System.out.println("No active workers found to terminate.");
        }
    }

    /**
 * Gets its own instance ID from the AWS Instance Metadata Service
 * and issues a termination request for itself.
 */
    private static void terminateSelf(Ec2Client ec2) {
        System.out.println("Terminating Manager (self)...");
        try {
            // Use the Instance Metadata Service to find our own ID
            // This is a special, non-routable IP address accessible only from the instance
            String command = "curl -s http://169.254.169.254/latest/meta-data/instance-id";
            Process process = Runtime.getRuntime().exec(command);
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            
            String instanceId = reader.readLine();
            reader.close();
            
            if (instanceId != null && !instanceId.isEmpty()) {
                System.out.println("My instance ID is: " + instanceId);
                TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                        .instanceIds(instanceId)
                        .build();
                ec2.terminateInstances(terminateRequest);
            } else {
                System.err.println("Could not get self instance-id to terminate.");
            }
        } catch (IOException e) {
            System.err.println("Error terminating self: " + e.getMessage());
        }
    }

    /**
     * Helper class to track the progress of a single job.
     */
    static class JobTracker {
        final List<String> results;
        final AtomicInteger tasksRemaining;
        
        public JobTracker(int totalTasks) {
            // Thread-safe list to hold the output lines
            this.results = Collections.synchronizedList(new ArrayList<>());
            // Thread-safe counter initialized to the total number of URLs
            this.tasksRemaining = new AtomicInteger(totalTasks);
        }
    
        public void addResult(String resultLine) {
            results.add(resultLine);
        }
    
        // Decrements the counter by 1 and returns the new value
        public int decrementAndGet() {
            return tasksRemaining.decrementAndGet();
        }
    }
}