package com.example;
 
import java.io.File;
import java.util.List;
import java.util.UUID;

import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.sqs.model.Message;
 
public class LocalApp {
 
    final static AWS aws = AWS.getInstance();
   
    // Unique ID for this specific run
    final static String localAppId = UUID.randomUUID().toString();
 
    // The NAME of the shared queue
    final static String MANAGER_QUEUE_NAME = "LocalToManager";
 
    public static void main(String[] args) {
 
        // --- 1. Argument Parsing ---
        if (args.length < 3) {
            System.err.println("Usage: java -jar LocalApp.jar <inputFileName> <outputFileName> <n> [terminate]");
            return;
        }
 
        String inputFileName = args[0];
        String outputFileName = args[1];
        int n;
        try {
            n = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.err.println("Error: 'n' (workers ratio) must be an integer.");
            return;
        }
        boolean terminate = args.length > 3 && "terminate".equalsIgnoreCase(args[3]);
 
        System.out.println("Starting Local Application...");
        System.out.println("App ID: " + localAppId);
 
        // Queue Configuration
        String responseQueueName = "ResponseQueue_" + localAppId;
        String managerQueueUrl = null;
        String responseQueueUrl = null;
 
        try {
            // --- 2. Setup S3 & Queues ---
            System.out.println("[DEBUG] Ensuring Bucket and Queues exist...");
            aws.createBucketIfNotExists(aws.bucketName);
 
            // Create/Get Shared Queue
            aws.createSqsQueue(MANAGER_QUEUE_NAME);
            managerQueueUrl = aws.getQueueUrl(MANAGER_QUEUE_NAME);
 
            // Create/Get Response Queue
            aws.createSqsQueue(responseQueueName);
            responseQueueUrl = aws.getQueueUrl(responseQueueName);
 
 
            // --- 3. Manager Bootstrapping ---
            System.out.println("[DEBUG] Checking for Manager...");
           
            Instance manager = aws.findManagerInstance();
           
            if (manager == null) {
                System.out.println("Manager not active. Starting new Manager instance...");
               
                String userDataScript = "#!/bin/bash\n" +
                        "yum update -y\n" +
                        "yum install java-17-amazon-corretto -y\n" +
                        "aws s3 cp s3://" + aws.bucketName + "/Manager.jar /home/ec2-user/Manager.jar\n" +
                        "java -cp /home/ec2-user/Manager.jar com.example.Manager > /home/ec2-user/manager.log 2>&1\n";
 
                // createEC2 returns a List<String>, we take the first ID
                List<String> instanceIds = aws.createEC2(userDataScript, "Manager", 1);
                aws.waitForInstance(instanceIds.get(0));
            } else {
                System.out.println("Manager is already active: " + manager.instanceId());
                if (manager.state().nameAsString().equals("stopped")) {
                    System.out.println("Manager is stopped. Waking it up...");
                    aws.startInstance(manager.instanceId());
                    aws.waitForInstance(manager.instanceId());
                }
            }
 
 
            // --- 4. Upload Input File to S3 ---
            String inputKey = "input_" + localAppId + ".txt";
            aws.uploadFileToS3(inputKey, new File(inputFileName));
 
 
            // --- 5. Send Task Message to Manager ---
            String messageBody = aws.bucketName + "," + inputKey + "," + n + "," + responseQueueName;
           
            aws.sendMessageToSQS(managerQueueUrl, messageBody);
           
            System.out.println("Task sent to Manager: " + messageBody);
            System.out.println("Waiting for results on " + responseQueueName + "...");
 
 
            // --- 6. Wait for Response ---
            boolean jobDone = false;
            while (!jobDone) {
                List<Message> messages = aws.receiveMessages(responseQueueUrl);
 
                if (messages.isEmpty()) {
                    System.out.print(".");
                    try { Thread.sleep(1000); } catch (InterruptedException e) {}
                    continue;
                }
 
                for (Message msg : messages) {
                    String body = msg.body();
                   
                    if (body.startsWith("done:")) {
                        // Extract URL
                        String s3Url = body.substring(5);
                        String s3OutputKey = s3Url;
                        if (s3Url.contains("/")) {
                            s3OutputKey = s3Url.substring(s3Url.lastIndexOf('/') + 1);
                        }
                       
                        System.out.println("\nJob Finished! Downloading result: " + s3OutputKey);
                       
                        // FIX: Only pass 2 arguments (key, path)
                        aws.downloadFileFromS3(s3OutputKey, outputFileName);
                        System.out.println("Result saved to: " + outputFileName);
                       
                        aws.deleteMessage(responseQueueUrl, msg.receiptHandle());
                        jobDone = true;
                        break;
                    }
                }
            }
 
 
            // --- 7. Terminate Manager (if requested) ---
            if (terminate) {
                System.out.println("Sending TERMINATE signal to Manager...");
                aws.sendMessageToSQS(managerQueueUrl, "TERMINATE");
            }
 
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // --- 8. Cleanup ---
            System.out.println("Cleaning up: Deleting temporary queue " + responseQueueName);
            aws.deleteQueue(responseQueueUrl);
        }
    }
}