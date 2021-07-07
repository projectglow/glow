##### The Glow documentation is published with [Read the Docs](https://readthedocs.org/).

To build the docs locally,
first create the conda environment:

`conda env create -f source/environment.yml`

activate the glow docs dev conda [environment](source/environment.yml):

`conda activate glow-docs`

build the docs:

`make livehtml`

connect to the local server via your browser at:

`http://127.0.0.1:8000`

when a change is detected in the docs, 
the documentation is rebuilt and any open browser windows are reloaded automatically.

deactivate the environment after you are done:

`conda deactivate`
