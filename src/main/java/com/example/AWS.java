package com.example;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.IamInstanceProfileSpecification;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.StartInstancesRequest;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.ec2.waiters.Ec2Waiter;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class AWS {
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;

    public static String ami = "ami-0fa3fe0fa7920f68e";

    public static Region region = Region.US_EAST_1;

    private static final AWS instance = new AWS();

    private AWS() {
        s3 = S3Client.builder().region(region).build();
        sqs = SqsClient.builder().region(region).build();
        ec2 = Ec2Client.builder().region(region).build();
    }

    public static AWS getInstance() {
        return instance;
    }

    public final String bucketName = "naser-wesam-dsp-bucket-v3";

    // S3
    public void createBucketIfNotExists(String bucketName) {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .build());

            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }

    // EC2
    public List<String> createEC2(String script, String tagName, int numberOfInstances) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_MICRO)
                .imageId(ami)
                .maxCount(numberOfInstances)
                .minCount(numberOfInstances)
                .keyName("vockey")
                .securityGroupIds("sg-026d8ac7682b22bc1")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString((script).getBytes())) 
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);

        // Collect ALL instance IDs
        List<String> instanceIds = new ArrayList<>();
        for (Instance instance : response.instances()) {
            instanceIds.add(instance.instanceId());
        }

        // Tag ALL instances
        Tag tag = Tag.builder()
                .key("Role")
                .value(tagName)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceIds)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf("[DEBUG] Successfully started %d EC2 instances with tag %s\n", instanceIds.size(), tagName);
        } catch (Ec2Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
        }
        
        return instanceIds; 
    }

    public void createSqsQueue(String queueName) {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        sqs.createQueue(createQueueRequest);
    }


    public Instance findManagerInstance() {
        // 1. Create a filter to find the instance by its tag
        Filter tagFilter = Filter.builder()
                .name("tag:Name") // The tag key
                .values("Manager") // The tag value
                .build();

        // 2. Create a filter to exclude terminated instances
        Filter stateFilter = Filter.builder()
                .name("instance-state-name")
                .values("pending", "running", "stopping", "stopped")
                .build();

        // 3. Build the request with both filters
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(tagFilter, stateFilter)
                .build();

        // 4. Call the AWS API
        DescribeInstancesResponse response = ec2.describeInstances(request);

        // 5. Check the result
        for (Reservation reservation : response.reservations()) {
            if (!reservation.instances().isEmpty()) { // sometimes aws send an empty reservation to dodge that case we do for loo instead of at(0)
                // Found it! Return the first one.
                return reservation.instances().get(0);
            }
        }

        // 6. No instances were found
        return null;
    }


    public void startInstance(String instanceId) {
        StartInstancesRequest request = StartInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();
        ec2.startInstances(request);
    }


    public void waitForInstance(String instanceId) {
        System.out.println("Waiting for instance " + instanceId + " to be 'running'...");

        // 1. Create a waiter object from the internal EC2 client
        Ec2Waiter ec2Waiter = this.ec2.waiter();

        // 2. Define what we are waiting for
        DescribeInstancesRequest waitRequest = DescribeInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        // 3. This line will pause your program and poll AWS
        //    until the instance is "running".
        WaiterResponse<DescribeInstancesResponse> waiterResponse = 
                ec2Waiter.waitUntilInstanceRunning(waitRequest);

        // Optional: Print the result from the waiter
        // waiterResponse.matched().response().ifPresent(System.out::println);

        System.out.println("Instance is now running!");
    }


    public void uploadFileToS3(String keyName, File file) {
        System.out.println("Uploading " + file.getName() + " to S3...");
        
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(this.bucketName) // Uses the bucketName already in your AWS class
                .key(keyName)
                .acl(ObjectCannedACL.PUBLIC_READ) // Optional: make the file public
                .build();

        // The s3 client is already defined as this.s3
        this.s3.putObject(request, RequestBody.fromFile(file));
        
        System.out.println("Upload complete.");
    }

    public void sendMessageToSQS(String queueUrl, String messageBody) {
        System.out.println("Sending SQS message to " + queueUrl + "...");

        // Build the request
        SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .build();

        // Send the message
        sqs.sendMessage(sendMsgRequest);
        System.out.println("Message sent.");
    }


    public List<Message> receiveMessages(String queueUrl) {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder().queueUrl(queueUrl)
            .maxNumberOfMessages(5) // Receive up to 5 messages at a time
            .waitTimeSeconds(20).build();    // Use 20-second long polling

        // The sqs client is already defined as this.sqs
        return this.sqs.receiveMessage(receiveRequest).messages();
    }

    public List<Message> receiveWorkerMessages(String queueUrl) {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
            .queueUrl(queueUrl)
            .maxNumberOfMessages(1)
            .waitTimeSeconds(20)  
            .visibilityTimeout(300)
            .build();

        return this.sqs.receiveMessage(receiveRequest).messages();
    }


    public void deleteMessage(String queueUrl, String receiptHandle) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
            .queueUrl(queueUrl)
            .receiptHandle(receiptHandle).build(); // The unique ID of the message to delete

        this.sqs.deleteMessage(deleteRequest);

    }

    public void deleteQueue(String queueUrl) {
        try {
            sqs.deleteQueue(software.amazon.awssdk.services.sqs.model.DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build());
            System.out.println("Deleted queue: " + queueUrl);
        } catch (Exception e) {
            System.err.println("Failed to delete queue: " + e.getMessage());
        }
    }
 
    public String getQueueUrl(String queueName) {
        GetQueueUrlResponse getQueueUrlResponse = sqs.getQueueUrl(
            GetQueueUrlRequest.builder().queueName(queueName).build()
        );
        return getQueueUrlResponse.queueUrl();
    }

    public void terminateInstance(TerminateInstancesRequest terminateRequest) {
        ec2.terminateInstances(terminateRequest);
    }

    public void downloadFileFromS3(String keyName, String outputFilePath) {
        System.out.println("Downloading " + keyName + " from S3...");
        software.amazon.awssdk.services.s3.model.GetObjectRequest request =
            software.amazon.awssdk.services.s3.model.GetObjectRequest.builder()
                .bucket(this.bucketName)
                .key(keyName)
                .build();
    
        this.s3.getObject(request, java.nio.file.Paths.get(outputFilePath));
        System.out.println("Download complete.");
    }

        // Efficient method for reading files without saving to disk
    public InputStream getFileStream(String bucketName, String keyName) {
        return s3.getObject(GetObjectRequest.builder()
                .bucket(bucketName)
                .key(keyName)
                .build());
    }

    public DescribeInstancesResponse describeInstancesRequest(DescribeInstancesRequest request) {
       return ec2.describeInstances(request);
    }
}