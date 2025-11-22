package com.example;

import java.io.File;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.StartInstancesRequest;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.sqs.model.Message;


public class App {

    final static AWS aws = AWS.getInstance();
    final static String jobId = UUID.randomUUID().toString();

    public static void main(String[] args) {
    
    
        if (args.length < 3) {
        System.err.println("Error: Please provide an input file path.");
        return;
    }
    System.out.println("This job's unique ID is: " + jobId);

    boolean terminate = args.length > 3 && args[3].equals("terminate");


    String InputFileName = args[0];
    String outputFileName = args[1];
    int n = Integer.parseInt(args[2]);


    //1-  this is the userdata
     String userDataScript = new StringBuilder()
            .append("#!/bin/bash\n")
            .append("yum update -y\n")
            .append("yum install java-1.8.0 -y\n")
            // TODO: Download your Manager.jar from S3  ( to do after manger)
            .append("aws s3 cp s3://your-s3-bucket-name/Manager.jar /home/ec2-user/Manager.jar\n")
            // TODO: Run your Manager
            .append("java -jar /home/ec2-user/Manager.jar\n") // to do after manger
            .toString();


    // we satrt a bucket 
    try {
            setup();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        


    //3 - we check the manger 
        Instance manager = aws.findManagerInstance(); 
        if (manager == null) { // if there is no manger
            System.out.println("No manager found. Creating a new one...");
            String instanceId = aws.createEC2(userDataScript,"Manager",1); // creat a new manger
            aws.waitForInstance(instanceId);
        }
        else {
            System.out.println("Found existing manager: " + manager.instanceId());
            if (manager.state().nameAsString().equals("stopped")) { // the state os an instance can be "running","pending", "stopped"
                System.out.println("Manager is stopped. Sending start command...");
                aws.startInstance(manager.instanceId());
                aws.waitForInstance(manager.instanceId());
        }}

        

        try {
        // Use the local file's name as the name in S3
        String keyName = new File(InputFileName).getName(); // not necessary
        aws.uploadFileToS3(keyName, InputFileName);

        System.out.println("Sending task to SQS...");


        Taskmessage task = new Taskmessage();
        task.setJobId(jobId);
        task.setInputFileKey(keyName);
        task.setOutputFileName(outputFileName);
        task.setN(n);


        ObjectMapper objectMapper = new ObjectMapper();
        String messageBody = objectMapper.writeValueAsString(task); //put in in a json
        aws.sendMessageToSQS("TasksQueue", messageBody);
        



        // we are done sending the message to the manger now we wait till we get a message from the mangere
        System.out.println("Waiting for 'Done' message on DoneQueue...");
        boolean jobDone = false;
        
        while (!jobDone) {
            // This will wait up to 20 seconds for a message
            List<Message> messages = aws.receiveMessages("DoneQueue");

            if (messages.isEmpty()) {
                System.out.println("...no message yet, polling again...");
                continue; // Go back to the start of the 'while' loop
            }

            for (Message msg : messages) {
                // 1. Get the message body (which is a JSON string)
                String doneBody = msg.body();
                
                // 2. Convert it back into a DoneMessage object
                Donemessage doneMessage = objectMapper.readValue(doneBody, Donemessage.class);

                // 3. IS THIS OURS? Check if the Job ID matches!
                if (jobId.equals(doneMessage.getJobId())) {
                    System.out.println("--- JOB COMPLETE ---");
                    
                    if(doneMessage.getError() != null) {
                        System.err.println("Job failed: " + doneMessage.getError());
                    } else {
                        System.out.println("Output file is ready at: " + doneMessage.getOutputS3Key());
                        System.out.println("Downloading output file...");
                        aws.downloadFileFromS3(doneMessage.getOutputS3Key(), outputFileName);
                        System.out.println("✅ Output saved to: " + outputFileName);
                    }
                    
                    // 4. Delete the message so we don't process it again
                    aws.deleteMessage("DoneQueue", msg.receiptHandle());
                    jobDone = true; // Set flag to exit the 'while' loop
                    break; // Exit the 'for' loop
                } else {
                    // This was another App's message. 
                    // We don't delete it. We just ignore it.
                    System.out.println("...received message for a different job, ignoring...");
                }
            }
        }

        if (terminate) {
            System.out.println("Terminate flag detected. Sending terminate message...");
            try {
                TerminateMessage termMsg = new TerminateMessage(); 
                ObjectMapper mapper = new ObjectMapper();
                String termBody = mapper.writeValueAsString(termMsg);
                
                // Send to the SAME queue that tasks go to
                aws.sendMessageToSQS("TasksQueue", termBody);
                System.out.println("Terminate message sent.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
            
    
    } catch (Exception e) {
        System.err.println("File upload failed: " + e.getMessage());
        e.printStackTrace();
    }
    

    }
    


     private static void setup() {
        System.out.println("[DEBUG] Create bucket if not exist.");
        aws.createBucketIfNotExists(aws.bucketName);

        System.out.println("[DEBUG] Create SQS queue if not exist.");
        aws.createSqsQueue("TasksQueue"); // Use a consistent name
        aws.createSqsQueue("DoneQueue");
    }
}
