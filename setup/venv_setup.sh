#! /usr/bin/bash

#WARNING! Before using this bash script, please run the following command on the directory this script is in if coming across permission issues.
#chmod u+x install.sh

#This bash script sets up a virtual environment for the repo
#Please make sure this script is located in the top level directory
echo "---Starting install..."

echo "---Please enter a name for the virtual environment"
read venv_name

#Install necessary pre-requisites
echo "---Installing virtual environment"
sudo apt-get -y install python3-pip
sudo apt-get -y install virtualenv
sudo apt-get -y install python3-venv

#Start by creating the virtual environment
echo "---Creating venv"
#You could either use this method or consider using virtualenv with this command "virtualenv venv1 venv2 venv3" should you wish to install multiple venv's
mkdir "$venv_name"
python3 -m venv ./"$venv_name"
python -m venv ./"$venv_name"

#Activate and upgrade the virutal environment
echo "---Now activating and upgrading venv's pip"
source ./"$venv_name"/bin/activate
pip install --upgrade pip

#Using poetry to install relevant libraries
echo "---Installing Python libraries via poetry"
pip install poetry
poetry lock
#Consider using "poetry install --no-dev" if you want to remove all the development version dependencies
poetry install

#Install and setup precommit hooks
echo "---Setting up precommit hooks"
pre-commit install
pre-commit autoupdate


echo "Installation complete"
