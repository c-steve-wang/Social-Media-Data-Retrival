import subprocess
import schedule
import time
import os
import signal

# Path to the child Python script
child_script = "waze_retrival.py"

# Global variable to keep track of the child process
child_process = None

def run_script():
    global child_process
    # Kill existing child process if exists
    if child_process is not None:
        os.kill(child_process.pid, signal.SIGTERM)
        child_process.wait()
    
    # Start the child script
    child_process = subprocess.Popen(["python", child_script])

def restart_script():
    print("Restarting script at midnight")
    run_script()

# Schedule the script restart at midnight
schedule.every().day.at("00:00").do(restart_script)

# Initial script run
run_script()

# Loop to keep the script running and check for scheduled tasks
while True:
    schedule.run_pending()
    time.sleep(1)
