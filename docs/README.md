#The Glow documentation is published with [Read the Docs](https://readthedocs.org/).

#To build the docs locally, activate the dev conda environment in `docs/source/environment.yml`
conda env create -f source/environment.yml
conda activate glow-docs
make livehtml

#connect to the local server via your browser at
http://127.0.0.1:8000

#When a change is detected in docs/, 
#the documentation is rebuilt and any open browser windows are reloaded automatically

#deactivate the environment after you are done
conda deactivate
