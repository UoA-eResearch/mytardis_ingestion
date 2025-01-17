#!/bin/bash
# Usage: ./run_biru_scan.sh <target_dir> <storage_dir>
# eg: ./run_biru_scan.sh /mnt/biru_shared_drive/MyTardisTestData/Demo_ingestion/ /srv/mytardis/data/

# Define target directory to be scanned  
target_dir="$1"
# Storage directory for files to be stored
storage_dir="$2"
# Ingestion script directory
INGESTION_DIRECTORY="/home/mytardis/mytardis_ingestion"

# File to check
file_to_check="ingestion.yaml"

# Log file
log_file="error.log"

# Check if target directory and storage directory are provided
if [ -z "$target_dir" ] || [ -z "$storage_dir" ]; then
    echo "Usage: $0 <target_directory> <storage_directory>"
    exit 1
fi

# Search for 'ingestion.yaml' files recursively
ingestion_yaml_files=($(find "$target_dir" -type f -name 'ingestion.yaml'))
echo $ingestion_yaml_files

# Check the number of found files
num_files=${#ingestion_yaml_files[@]}

# Check if "ingestion.yaml" exists in the target directory
if [ $num_files -eq 0 ]; then
    echo "No 'ingestion.yaml' files found."

elif [ $num_files -eq 1 ]; then
	# Load YAML file
	ingestion_yaml_file="${ingestion_yaml_files[0]}"
    yaml_data=$(yq eval '.' "$ingestion_yaml_file")
	
    # Check if any data_status field is empty or None
    data_status_empty=$(echo "$yaml_data" | yq eval 'select(.data_status == "" or .data_status == null)' -)

	# If any data_status field is empty or None, proceed with ingestion
    if [ -n "$data_status_empty" ]; then
        # Execute the command
		# Change directory to /home/mytardis/mytardis_ingestion
		cd $INGESTION_DIRECTORY
        
		# Run the scan script
		poetry run python3 src/ingestion_biru.py "$ingestion_yaml_file" "$storage_dir" unix_fs

		# Display a message indicating completion
		echo "Script completed."
	else
        echo "No new objects need to be ingested."
    fi

else
    # Multiple 'ingestion.yaml' files found, inform the user
    echo "Multiple 'ingestion.yaml' files found. Please ensure there is only one."
    echo "Found files:"
    printf "%s\n" "${ingestion_yaml_files[@]}"
