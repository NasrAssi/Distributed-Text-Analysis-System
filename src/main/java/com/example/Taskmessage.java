package com.example;

public class Taskmessage {
    private String jobId;
    private String keyName;
    private String outputFileName;
    private int N;

    public Taskmessage(String keyName, String outputFileName,int n){
        this.keyName = keyName;
        this.outputFileName = outputFileName;
        this.N = n;
    }
    public Taskmessage(){}

    public void setInputFileKey(String keyName) {
        this.keyName = keyName;
    }
    public void setN(int n) {
        this.N = n;
    }
    public void setOutputFileName(String outputFileName) {
        this.outputFileName = outputFileName;
    }
    public String getJobId() { 
        return jobId; 
    }
    public void setJobId(String jobId) { 
        this.jobId = jobId; 
    }
    






}
