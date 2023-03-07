#Installs the necessary extensions required for VS Code

#Run the following command if you wish to get list of extensions installed for Unix
#code --list-extensions | xargs -L 1 echo code --install-extension

#Run the following command if you wish to get list of extensions installed for Windows
# (using VS Code's integrated terminal)
#code --list-extensions | % { "code --install-extension $_" }

code --install-extension donjayamanne.python-environment-manager
code --install-extension GitHub.vscode-pull-request-github
code --install-extension ms-python.black-formatter
code --install-extension ms-python.isort
code --install-extension ms-python.pylint
code --install-extension ms-python.python
code --install-extension ms-python.vscode-pylance
code --install-extension ms-toolsai.jupyter
code --install-extension ms-toolsai.jupyter-keymap
code --install-extension ms-toolsai.jupyter-renderers
code --install-extension ms-toolsai.vscode-jupyter-cell-tags
code --install-extension ms-toolsai.vscode-jupyter-slideshow
code --install-extension ms-vscode-remote.remote-ssh
code --install-extension ms-vscode-remote.remote-ssh-edit
code --install-extension ms-vscode.remote-explorer
code --install-extension njpwerner.autodocstring
code --install-extension redhat.vscode-xml
code --install-extension redhat.vscode-yaml
code --install-extension zeshuaro.vscode-python-poetry