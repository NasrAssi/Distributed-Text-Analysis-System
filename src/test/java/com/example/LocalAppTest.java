package com.example;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class LocalAppTest {

    // Point to your ACTUAL input file
    // Make sure this path matches exactly where it is in your VS Code folder
    private static final String INPUT_FILE_PATH = "C:\\Users\\naser\\Desktop\\study\\DSP\\DSP1\\demo\\input.text";
    private static final String OUTPUT_FILE_NAME = "test_output.html";
    private static final String WORKERS_PER_N = "1"; // n = 1

    @Test
    @Timeout(value = 60, unit = TimeUnit.MINUTES) // Give it 60 mins just in case big data takes time
    public void testSystemWithRealInput() {
        System.out.println("[TEST] Starting System Test using file: " + INPUT_FILE_PATH);

        // 1. Verify the input file actually exists before starting
        File inputFile = new File(INPUT_FILE_PATH);
        if (!inputFile.exists()) {
            fail("Could not find input file at: " + inputFile.getAbsolutePath());
        }

        // 2. Define arguments: input, output, n
        String[] args = {
            INPUT_FILE_PATH, 
            OUTPUT_FILE_NAME, 
            WORKERS_PER_N
        };

        // 3. Run the Local Application
        // This will upload your specific file, start the manager, and wait for results.
        try {
            // Assuming your LocalApp class is named "LocalApp" or "App"
            // Based on your previous messages, your main class is called "App"
            LocalApp.main(args); 
        } catch (Exception e) {
            e.printStackTrace();
            fail("Application crashed: " + e.getMessage());
        }

        // 4. Verify the output HTML was created
        File outputFile = new File(OUTPUT_FILE_NAME);
        assertTrue(outputFile.exists(), "The output HTML file was not created!");
        assertTrue(outputFile.length() > 0, "The output HTML file is empty!");
        
        System.out.println("[TEST] Success! View results at: " + outputFile.getAbsolutePath());
    }

    @AfterEach
    public void cleanup() {
        // Only delete the OUTPUT file. 
        // Do NOT delete the input file since it is your real source file.
        File outputFile = new File(OUTPUT_FILE_NAME);
        if (outputFile.exists()) {
            // outputFile.delete(); // Uncomment if you want to auto-delete the result
        }
    }
}