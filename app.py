import subprocess

# Execute the commands with delay
subprocess.run("python3 voting.py & sleep 20 & python3 spark-streaming.py & sleep 20 & streamlit run streamlit-app.py", shell=True)
