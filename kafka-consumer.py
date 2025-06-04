import subprocess

command = ["docker", "exec", "kafka-iot", "kafka-console-consumer",
           "--bootstrap-server", "localhost:9092",
           "--topic", "clickstream",
           "--from-beginning"]

try:
    # This is to ensure the process is properly terminated
    process = subprocess.Popen(command, 
                             stdout=subprocess.PIPE, 
                             stderr=subprocess.PIPE, 
                             universal_newlines=True)
    
    print("Kafka consumer started. Listening for messages...")
    
    # Read output line by line (non-blocking)
    while True:
        output = process.stdout.readline()
        if output:
            print(output.strip())
        
        # Check if process is still running 
        if process.poll() is not None:
            break
                      
except KeyboardInterrupt:
    print("Kafka consumer stopped by user.")
    if process:
        process.terminate()
except Exception as e:
    print(f"Error: {e}")
finally:
    if process:
        process.terminate()