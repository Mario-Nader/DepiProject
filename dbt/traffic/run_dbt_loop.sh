source /c/Users/ammar/Desktop/DepiProject/dbt/traffic/venv/Scripts/activate

cd /c/Users/ammar/Desktop/DepiProject/dbt/traffic

nohup bash -c '
while true
do
    echo "Running dbt run..."
    dbt run
    echo "Sleeping for 30 seconds..."
    sleep 30
done
' > dbt_output.log 2>&1 &

echo "dbt loop is now running in the background. Output is in dbt_output.log"
