source /c/Users/ammar/Desktop/DepiProject/venv/Scripts/activate

cd /c/Users/ammar/Desktop/DepiProject

nohup python csv_upload.py > upload_output.log 2>&1 &
echo "Upload script is running in the background..."
