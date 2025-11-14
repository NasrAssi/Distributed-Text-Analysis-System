package com.example;

public class Taskmessage {
    private String messageType;
    private String jobId;
    private String s3Pointer;

    public Taskmessage(String messageType, String jobId, String s3Pointer){
        this.messageType = messageType;
        this.jobId = jobId;
        this.s3Pointer = s3Pointer;
    }
    public Taskmessage(){}

    public String getJobId() {
        return this.jobId;
    }
    






}
