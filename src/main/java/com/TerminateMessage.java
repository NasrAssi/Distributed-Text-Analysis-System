package com.example;

public class TerminateMessage {
    private String type = "TERMINATE"; // Helps the Manager know what this is

    public TerminateMessage() {}

    public String getType() {
        return type; 
    }
    public void setType(String type) { 
        this.type = type; 
    }
}