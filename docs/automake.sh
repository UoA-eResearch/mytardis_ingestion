#Automates the process of creating documentation of the project in sphinx
#This script is to be called within the docs directory

echo "---Starting auto doc generation..."

echo "---Creating output files for apidoc"
sphinx-apidoc -o ./source ../src

echo "---Generating html files"
make html

echo "---Generating PDF files"
make latexpdf