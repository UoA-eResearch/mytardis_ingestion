
# Development Environment Setup

The setup is designed for an Ubuntu/Linux (recommended) or Manjaro/Linux setup with VS Code installed. For a generalised step-by-step setup for different OS environments, please refer to the Documentation channel on Teams, otherwise continue to see the following summarised steps (please note that the VS Code extensions are optional):

## Repo - Ubuntu/Manjaro

1.	Generate and add an SSH key for the device this repo is to be downloaded in on GitHub.
2.	Clone this repository via SSH.
3. On a terminal, cd into this repo's directory, and run the following command “source ./setup/<yourOS>_venv-setup.sh”, where <yourOS> is the operating system being used by the dev. A prompt will be displayed to enter the sudo password (if applicable). Please enter this to run the rest of the script. This will create and setup a virtual environment for the repo via the poetry method, and will also install pre-commit.
4.	Now the terminal is ready to run project scripts.

 ## Repo - Generalised Instructions

These instructions are written for non Ubuntu OS environments
1.	Install git, Python 3, and curl that can be run on a terminal. rsync are optionally required to run some tests.
2.	Create an SSH key for the device to link it to the dev’s GitHub account.
3.	Clone the mytardis_ingestion repo in the desired directory. This can be achieved on the terminal by first changing into the desired directory, then running the command “git clone git@github.com:UoA-eResearch/mytardis_ingestion.git”
4.	On the terminal, run the following command “curl -sSL https://install.python-poetry.org | python3 –“. This will install Poetry in the OS.
5.	Add the Poetry to the PATH environment variable.
6.	Run the following commands in this order (the explanation for each command is defined in the line above that command that start with #, and has been written to be friendly with terminal/bash):
  ```bash
	  # create a local virtual environment in the project directory
	  poetry config virtualenvs.in-project true
	  # in this case this command sets up the poetry file for installation
	  poetry lock
	  # installs all the relevant libraries for Python
	  poetry install
	  # installs git pre-commit
	  poetry run pre-commit install
	  # updates any relevant dependencies, used to avoid dependency issues
	  poetry run pre-commit autoupdate
  ```
7.	Run the command “poetry shell” to activate the virtual environment and the repo is now setup and ready to run the project.

 ## VS Code (OPTIONAL, and only recommended if this is a fresh install of the repo)

  1.  Open the “vscode_extensions.txt” file (found in the ./setup folder) and copy the commands for installing the extensions.
  2.	Using the terminal (or VS Code's terminal), paste the commands here and press the Enter key to install the relevant extensions required for VS Code.
