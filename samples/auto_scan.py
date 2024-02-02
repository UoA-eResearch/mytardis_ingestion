# bandit:disable B404
""" This is a sample script for automatically scanning a directory for 'ingestion.yaml'
and running ingestion if found.
"""
import os
import subprocess  # nosec
import time


class DirectoryScanner:
    """
    A class for scanning a directory, checking for the presence of an 'ingestion.yaml' file,
    and running an ingestion script if the file is found.
    """

    def __init__(self, directory: str):
        """
        Initialize the DirectoryScanner instance.

        Args:
            target_directory (str): The path to the target directory for scanning.
        """
        self.target_directory = directory

    def check_and_ingest(self) -> None:
        """
        Check for the presence of 'ingestion.yaml' in the target directory and run ingestion
        if found.
        """
        yaml_path = os.path.join(self.target_directory, "ingestion.yaml")

        if os.path.isfile(yaml_path):
            print(f"Found ingestion.yaml in {self.target_directory}")
            self.run_ingestion_script(yaml_path)
        else:
            print(f"No ingestion.yaml found in {self.target_directory}")

    def run_ingestion_script(self, yaml_path: str) -> None:
        """
        Run the ingestion script with the specified YAML file.

        Args:
            yaml_path (str): The path to the YAML file.
        """
        # Change this to the actual path of your rsync destination
        rsync_pth = "/Users/xli677/Desktop/mytardis_ingestion/ingestion_test/"
        print(f"Running ingestion for {yaml_path}")
        # Replace 'ingestion_biru_example.py' with the actual name of your ingestion script
        try:
            result = subprocess.run(  # nosec
                ["python", "samples/ingestion_biru_example.py", yaml_path, rsync_pth],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            print("Subprocess output:", result.stdout)
        except subprocess.CalledProcessError as e:  # nosec
            print("Subprocess error:", e.stderr)
            raise

    def scan_and_check(self) -> None:
        """
        Continuously scan the target directory for 'ingestion.yaml' and run ingestion
        if found.
        """
        while True:
            self.check_and_ingest()
            # Sleep for a week before the next scan
            time.sleep(7 * 24 * 60 * 60)  # 7 days * 24 hours * 60 minutes * 60 seconds


if __name__ == "__main__":
    # Change this to the actual path of your target directory
    TARGET_DIRECTORY = (
        "/Volumes/resmed202000005-biru-shared-drive/"
        "MyTardisTestData/Haruna_HierarchyStructure"
    )
    scanner = DirectoryScanner(TARGET_DIRECTORY)
    scanner.scan_and_check()
