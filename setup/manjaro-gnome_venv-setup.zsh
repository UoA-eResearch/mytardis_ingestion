#! /usr/bin/bash

#WARNING! Before using this bash script, please run the following command on the directory this script is in if coming across permission issues.
#chmod u+x install.sh

#This bash script sets up a virtual environment for the repo

#Please make sure that this script is called from the top level directory using the source command
#e.g. source ./setup/manjaro-gnome_venv-setup.zsh


echo "---Starting install..."


echo "---Installing poetry"
curl -sSL https://install.python-poetry.org | python3 -

echo "---Adding poetry to PATH and saving to bashrc"
export PATH="/home/user/.local/bin:$PATH"
echo '' >> ~/.bashrc
echo 'export PATH="/home/user/.local/bin:$PATH"' >> ~/.bashrc

echo "---Configuring poetry to create local .venv directory"
poetry config virtualenvs.in-project true

echo "---Setting up virtual environment in .venv"
poetry lock
poetry install

echo "---Setting up precommit hooks"
poetry run pre-commit install
poetry run pre-commit autoupdate

echo "---Setup complete! To use the venv, use the 'poetry shell' command."