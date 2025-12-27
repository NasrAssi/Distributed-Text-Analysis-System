DISTRIBUTED TEXT ANALYSIS SYSTEM ON AWS
======================================

Student:
  Wesam gara [213305741]
  Nasr assi [325707180]

Course: Distributed Systems Programming – Text Analysis in the Cloud


1. ENVIRONMENT / AWS DETAILS
----------------------------

Region:             us-east-1 (N. Virginia)

AMI used:
  - AMI ID:         ami-0fa3fe0fa7920f68e
  - Description:    Amazon Linux 2023 (Compatible with yum / java-17).

Instance types:
  - Manager type:   t2.micro
  - Worker type:    t2.micro
  - Max workers:    18 (Enforced by Manager logic to avoid AWS limits)

S3 bucket:
  - Name:           naser-wesam-dsp-bucket-v2
  - Structure:
      /             – Root folder contains Manager.jar and Worker.jar
      /             – Input files (e.g., input_UUID.txt)
      /             – Result files (e.g., output_UUID.txt)
      /             – Summary HTML files (e.g., summary_UUID.html)

SQS queues:
  - Local -> Manager:    LocalToManager
  - Manager -> Worker:   ManagerToWorker
  - Worker -> Manager:   WorkerToManager
  - Manager -> Local:    ResponseQueue_[UUID] (for each LocalApp)


2. HOW TO BUILD
---------------

From the project root directory:

1. Build the Fat JARs using Maven:
   
   mvn clean package

   This creates the artifact in the `target/` folder (e.g., `demo-1.0-SNAPSHOT.jar`).

2. Rename and Upload JARs to S3:
   The EC2 instances expect specific filenames in the root of the bucket.

   a. Copy `target/demo-1.0-SNAPSHOT.jar` to `Manager.jar` and upload to S3 root.
   b. Copy `target/demo-1.0-SNAPSHOT.jar` to `Worker.jar` and upload to S3 root.
   
   *Note: Ensure these are uploaded to 's3://naser-wesam-dsp-bucket-v2/' directly, not in a subfolder.*


3. INPUT FORMAT
---------------

The input file is a plain text file. Each line contains an operation and a URL:

   <ANALYSIS_TYPE> <TAB> <URL>

Types: POS, CONSTITUENCY, DEPENDENCY.
Example:
   POS	http://alice.gutenberg.org/files/11/11-0.txt
   DEPENDENCY	http://www.gutenberg.org/files/1342/1342-0.txt


4. HOW TO RUN (LOCAL APP)
-------------------------

Command syntax:
   java -jar target/LocalApp.jar <input-file> <output-file> <n> [terminate]

Example command:
   java -jar target/dsp1-1.0-SNAPSHOT.jar input.txt output.html 1 terminate

Arguments:
  - input.txt:    Path to local input file.
  - output.html:  Path where the final HTML summary will be saved.
  - 1:            'n' (Workers per task ratio). 
                  (Logic: Workers = ceil(Lines / n)).
  - terminate:    (Optional) Sends a termination signal to the Manager after completion.


5. HIGH-LEVEL SYSTEM FLOW
-------------------------

5.1 Local Application
---------------------
1. Generates a unique ID (UUID) and creates a temporary, dynamic SQS queue (`ResponseQueue_UUID`) for this specific run.
2. Checks for an active Manager instance. If missing, launches a `t2.micro` instance with a User Data script that downloads `Manager.jar` from S3.
3. Uploads the input file to S3.
4. Sends a message to `LocalToManager` queue containing: Bucket, Key, N, and the Dynamic Response Queue URL.
5. Polls its private Response Queue. Upon receiving "done", it downloads the HTML summary and deletes the temporary queue.

5.2 Manager
-----------
1. Multithreaded: Uses an `ExecutorService` to handle multiple LocalApp requests in parallel.
2. Downloads input file from S3 using efficient Streams.
3. Scaling: Checks active workers and launches new `t2.micro` instances if needed (Logic: `required - active`, capped at 18 total).
4. Sends individual tasks to `ManagerToWorker` queue.
5. Results Collector: A separate thread polls `WorkerToManager`.
   - It aggregates results into a `JobTracker` object.
   - When all tasks for a specific job are done, it generates the HTML.
6. Uploads HTML to S3 and sends the S3 link to the *specific* `ResponseQueue` URL requested by the LocalApp.

5.3 Worker
----------
1. Bootstraps via User Data script (Updates yum, installs Java 17, downloads `Worker.jar`).
2. Long-polls `ManagerToWorker` queue.
3. Streaming Processing: 
   - Downloads the text file line-by-line using `BufferedReader` and `URL.openStream()`.
   - **Crucial:** It never loads the entire book into RAM, preventing OutOfMemory errors on `t2.micro` instances.
4. Processes each line with Stanford CoreNLP.
5. Writes results to a local temp file, uploads to S3, and notifies Manager.


6. RUN STATISTICS (TEST RUN)
----------------------------
  - Input:          input.txt
  - Instance Type:  t2.micro
  - Workers:        1 (n=1)
  - Time to finish: 11:47 minutes (dominated by cold boot/install time)
  - Result:         Successful HTML generation.


7. DESIGN DECISIONS
-------------------

1. MULTI-CLIENT SUPPORT (Unique Feature):
   - Unlike basic implementations that use one static queue for responses, my LocalApp creates a **temporary, unique SQS queue** for every run.
   - The Manager reads this "Reply-To" address from the message.
   - This allows multiple clients (or multiple runs) to coexist without stealing each other's results.

2. MEMORY EFFICIENCY (STREAMING):
   - Project Gutenberg books can be large. Loading a whole book into a String variable causes crashes on small instances.
   - **Solution:** The Worker implementation uses `BufferedReader` to stream the file line-by-line from the URL, analyze it, and write it to disk immediately. The entire file is never held in RAM.

3. ROBUSTNESS:
   - **Job Tracking:** The Manager uses an `AtomicInteger` and `ConcurrentMap` to track exactly how many lines are outstanding for each job.
   - **Graceful Cleanup:** The system supports a `terminate` command which cleans up all Workers and the Manager itself using the Instance Metadata Service to self-identify.

4. SECURITY:
   - Uses `IamInstanceProfile` ("LabInstanceProfile") for EC2.
   - Uses `DefaultCredentialsProvider` for LocalApp.
   - No hardcoded secret keys.


8. THEORETICAL CONSIDERATIONS & SCALABILITY
-------------------------------------------

Fault Tolerance: To handle node failures (e.g., if a Worker crashes mid-process), we rely on SQS Visibility Timeouts. If a worker pulls a message but does not delete it (due to a crash), the message will become visible again after the timeout period. Another worker will then pick it up, ensuring no task is lost.