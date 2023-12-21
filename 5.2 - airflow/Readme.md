###Task 1 - Define the DAG arguments
See dag_args.jpg

###Task 2 - Define the DAG
The task was to create a DAG named process_web_log that runs daily.
See dag_definition.jpg

###Task 3 - Create a task to extract data
Task was to extract the ipaddress field from accesslog.txt and save it to extracted_data.txt
See extract_data.jpg

###Task 4 - Create a task to transform the data in the txt file
Task was to remove all occurences of "198.46.149.143” from the extracted data.
See transformed_data.jpg

###Task 5 - Create a task to load the data
Task was to load data into a tar file named weblog.tar
See load_data.jpg

###Task 6 - Define the task pipeline
Task was to define the pipeline
See pipeline.jpg

###Task 7 - Submit the DAG
Shows the command used to submit dag
See submit_dag.jpg

###Task 8 - Unpause the DAG
Shows the command used to unpause the dag
See unpause_dag.jpg

###Task 9 - Monitor the DAG
Shows the dag run in airflow console
See dag_runs.jpg
