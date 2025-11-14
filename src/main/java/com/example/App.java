package com.example;

import java.util.Base64;
import java.util.List;

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


public class App {
    public static void main(String[] args) {
        Ec2Client ec2 = Ec2Client.create();
        Instance manager = findManagerInstance(ec2); 
        if (manager == null) { // if there is no manger
            System.out.println("No manager found. Creating a new one...");
            CreateInstance(ec2); // creat a new manger
        } 
        else {
            System.out.println("Found existing manager: " + manager.instanceId());
            if (manager.state().nameAsString().equals("stopped")) { // the state os an instance can be "running","pending", "stopped"
                System.out.println("Manager is stopped. Sending start command...");
                startInstance(ec2, manager.instanceId());
        }}
        ec2.close();



    }

public static void CreateInstance(Ec2Client ec2) {
     
    String amiId = "ami-076515f20540e6e0b"; // Linux and Java 1.8

    // we should make sure this ami i close to us in our region

    //  USER DATA SCRIPT 
    

    String userDataScript = new StringBuilder()
        .append("#!/bin/bash\n")
        .append("yum update -y\n")
        .append("yum install java-1.8.0 -y\n")
        // TODO: Download your Manager.jar from S3  ( to do after manger)
        .append("aws s3 cp s3://your-s3-bucket-name/Manager.jar /home/ec2-user/Manager.jar\n")
        // TODO: Run your Manager
        .append("java -jar /home/ec2-user/Manager.jar\n") // to do after manger
        .toString();

    String userDataBase64 = Base64.getEncoder().encodeToString(userDataScript.getBytes());

    //  BUILD THE RUN REQUEST ( this is the code we took from the lectures and refined it)
    RunInstancesRequest runRequest = RunInstancesRequest.builder()
        .imageId(amiId) 
        .instanceType(InstanceType.T2_MICRO) // <-- FIXED: T1_MICRO is obsolete
        .maxCount(1)
        .minCount(1)
        .userData(userDataBase64) // <-- FIXED: Using the real script
        
        // TODO: Add your .pem key name so you can log in
        .keyName("your-key-pair-name") 
        
        // TODO: Add a security group that allows traffic 
        .securityGroupIds("sg-your-security-group-id") 
        // --- END CRITICAL ADDITIONS ---
        
        .build();

    RunInstancesResponse response = ec2.runInstances(runRequest);
    String instanceId = response.instances().get(0).instanceId();
    System.out.println("🚀 Successfully launched new Manager instance: " + instanceId);

    // --- 3. TAG THE INSTANCE (as per your lecture) ---
    // This is so you can find it next time.
    Tag tag = Tag.builder()
        .key("Role") // <-- The tag key
        .value("ManagerNode") // <-- The tag value
        .build();

    CreateTagsRequest tagRequest = CreateTagsRequest.builder()
        .resources(instanceId)
        .tags(tag)
        .build();

    ec2.createTags(tagRequest);
    System.out.println("🏷️  Successfully tagged instance " + instanceId);
    
    // Note: The List<Instance> instances = response.instances(); 
    // line is fine, but instanceId is what you really need.
}


private static Instance findManagerInstance(Ec2Client ec2) {
    // 1. Create a filter to find the instance by its tag
    Filter tagFilter = Filter.builder()
            .name("tag:Role") // The tag key
            .values("ManagerNode") // The tag value
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
        if (!reservation.instances().isEmpty()) {
            // Found it! Return the first one.
            return reservation.instances().get(0);
        }
    }

    // 6. No instances were found
    return null;
}


public static void startInstance(Ec2Client ec2, String instanceId) {
    StartInstancesRequest request = StartInstancesRequest.builder()
            .instanceIds(instanceId)
            .build();
    ec2.startInstances(request);


}
}
