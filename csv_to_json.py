import os
import json
import pandas as pd

def convert_files_to_json(folder_path):
    """
    Reads all files in a folder and converts their content to individual JSON files.
    This example assumes the content of each file is treated as a single string.
    """
    
    # Iterate through all items in the directory
    for filename in os.listdir(folder_path):
        # Construct the full path for the input file
        input_filepath = os.path.join(folder_path, filename)

        # Ensure it is a file and not a directory
        if os.path.isfile(input_filepath):
            # Read the file's content
            try:
                df = pd.read_csv(input_filepath)
            except Exception as e:
                print(f"Error reading file {filename}: {e}")
                continue

            # Define the output JSON filename (change extension)
            # This creates a new filename with '.json' extension in the same folder
            output_filename = os.path.splitext(filename)[0] + '.json'
            output_filepath = os.path.join(folder_path, output_filename)

            # Convert the Python object (the content string) to JSON format
            # and write it to a new file
            try:
                df.to_json(output_filepath, orient='records', indent=4)
                print(f"Successfully converted {filename} to {output_filename}")
            except IOError as e:
                print(f"Error writing JSON file for {filename}: {e}")

# Specify the path to your folder
# Example for a folder named 'my_files' in the current working directory
# You can change this to an absolute path like 'C:/Users/YourUser/Desktop/data'
folder_to_process = 'LMS-DataStuff/trove-periodicals-data-main' 

convert_files_to_json(folder_to_process)
