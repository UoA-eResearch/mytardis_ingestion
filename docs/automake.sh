#Automates the process of creating documentation of the project in sphinx
#This script is to be called within the docs directory using the source command

echo "---Starting auto doc generation..."

echo "---Cleaning current build"
make clean

echo "---Creating output files for apidoc"
sphinx-apidoc -o ./source ../src

echo "---Generating HTML files"
make html

echo "---Auto doc generation finished"