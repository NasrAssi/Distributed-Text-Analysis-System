package com.example;

public class Donemessage {
    private String jobId; 
    private String outputS3Key;
    private String error;


    public String getJobId() {
         return jobId;
    }
    public void setJobId(String jobId) {
        this.jobId = jobId; 
    }
    
    public String getOutputS3Key() { 
        return outputS3Key;
    }
    public void setOutputS3Key(String outputS3Key) {
        this.outputS3Key = outputS3Key;
    }
    
    public String getError() { 
        return error; 
    }
    public void setError(String error) { 
        this.error = error;
    }
}