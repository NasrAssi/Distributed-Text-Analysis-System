package com.example;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.waiters.Ec2Waiter;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.File;
import java.util.Base64;
import java.util.List;

public class AWS {
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;

    public static String ami = "ami-00e95a9222311e8ed";

    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;

    private static final AWS instance = new AWS();

    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region2).build();
    }

    public static AWS getInstance() {
        return instance;
    }

    public String bucketName = "only-together2";


    // S3
    public void createBucketIfNotExists(String bucketName) {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }

    // EC2
    public String createEC2(String script, String tagName, int numberOfInstances) {
        RunInstancesRequest runRequest = (RunInstancesRequest) RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_MICRO)
                .imageId(ami)
                .maxCount(numberOfInstances)
                .minCount(1)
                .keyName("vockey")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString((script).getBytes()))
                .build();


        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        software.amazon.awssdk.services.ec2.model.Tag tag = Tag.builder()
                .key("Name")
                .value(tagName)//////////////
                .build();

        CreateTagsRequest tagRequest = (CreateTagsRequest) CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "[DEBUG] Successfully started EC2 instance %s based on AMI %s\n",
                    instanceId, ami);

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            System.exit(1);
        }
        return instanceId;
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




public void uploadFileToS3(String keyName, String filePath) {
    System.out.println("Uploading " + filePath + " to S3...");
    
    PutObjectRequest request = PutObjectRequest.builder()
            .bucket(this.bucketName) // Uses the bucketName already in your AWS class
            .key(keyName)
            .build();

    // The s3 client is already defined as this.s3
    this.s3.putObject(request, RequestBody.fromFile(new File(filePath)));
    
    System.out.println("Upload complete.");
}

public void sendMessageToSQS(String queueName, String messageBody) {
    System.out.println("Sending SQS message to " + queueName + "...");

    // SQS requires you to get the queue's URL first
    GetQueueUrlResponse getQueueUrlResponse = this.sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
    String queueUrl = getQueueUrlResponse.queueUrl();

    // Build the request
    SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
            .queueUrl(queueUrl)
            .messageBody(messageBody)
            .build();

    // Send the message
    sqs.sendMessage(sendMsgRequest);
    System.out.println("Message sent.");
    }




    public List<Message> receiveMessages(String queueName) {
    // Get the queue URL
    GetQueueUrlResponse getQueueUrlResponse = this.sqs.getQueueUrl(
        GetQueueUrlRequest.builder().queueName(queueName).build());
    String queueUrl = getQueueUrlResponse.queueUrl();

    // Create a receive request with long polling
    ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder().queueUrl(queueUrl)
        .maxNumberOfMessages(5) // Receive up to 5 messages at a time
        .waitTimeSeconds(20).build();    // Use 20-second long polling

    // The sqs client is already defined as this.sqs
    return this.sqs.receiveMessage(receiveRequest).messages();
}


public void deleteMessage(String queueName, String receiptHandle) {
    // Get the queue URL
    GetQueueUrlResponse getQueueUrlResponse = this.sqs.getQueueUrl(
        GetQueueUrlRequest.builder().queueName(queueName).build()
    );
    String queueUrl = getQueueUrlResponse.queueUrl();

    DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
    .queueUrl(queueUrl)
    .receiptHandle(receiptHandle).build(); // The unique ID of the message to delete

    this.sqs.deleteMessage(deleteRequest);

}



}
