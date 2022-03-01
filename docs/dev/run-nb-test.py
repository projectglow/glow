'''
Runs all Glow notebooks as an integration test in Databricks using 
  - Databricks CLI to set up continuous integration
  - Repos to sync notebooks from the Glow repository to your workspace https://docs.databricks.com/dev-tools/cli/repos-cli.html#repos-cli
  - Multitask workflows to define the pipeline and run using Databricks Jobs API 2.1

The Glow workflow including all the notebooks is tested in a nightly integration test in Databricks.
This test requires the `Databricks CLI <https://docs.databricks.com/dev-tools/cli/index.html#set-up-authentication>`_ be configured with `Jobs API 2.1 <https://docs.databricks.com/dev-tools/cli/jobs-cli.html#requirements-to-call-the-jobs-rest-api-21>`_.

Example usage

  python3 docs/dev/run-nb-test.py --cli-profile <databricks cli profile> \
                                  --workflow-definition <configuration json> \
                                  --repos-path <path on databricks workspace> \
                                  --repos-url <git repo url> \
                                  --branch <git branch> \
                                  --dockerhub_password <Password for projectglow dockerhub account>

If you add notebooks or rename them, please also edit the workflow definition json located in this directory

The integration test uses the `Glow Docker Container <https://hub.docker.com/r/projectglow/databricks-glow>`_ to manage the environment. This container is authenticated with a password but can also be run with Default authentication by removing `basic_auth` from the workflow definition.

Once the notebook test run is kicked off, the output will look like this:

  Importing source files from Glow repo
  Create job
  Run job
  Check job status
  === Status check at XX:XX:XX ===
  (Run ID [RUNNING])
  ...
  ...

'''
import click
from datetime import datetime
import glob
import json
import os
import subprocess
import sys
import time
import uuid

def run_cli_cmd(cli_profile, api, args):
    cmd = ['databricks', '--profile', cli_profile, api] + args
    res = subprocess.run(cmd, capture_output=True)
    if res.returncode != 0:
        raise ValueError(res)
    return res.stdout

@click.command()
@click.option('--cli-profile', default='DEFAULT', help='Databricks CLI profile name.')
@click.option('--workflow-definition', default='docs/dev/multitask-integration-test-config.json', help='path to json configuration')
@click.option('--repos-path', default='/Repos/staging', help='Path in Databricks workspace for running integration test')
@click.option('--repos-url', default='https://github.com/projectglow/glow', help='URL for your fork of glow')
@click.option('--branch', default='master', help='Update to your branch that you are testing on')
@click.option('--dockerhub_password', default='DEFAULT', help='Password for projectglow dockerhub account')
def main(cli_profile, workflow_definition, repos_path, repos_url, branch, dockerhub_password):
    click.echo("cli_profile = " + cli_profile)
    click.echo("workflow_definition = " + workflow_definition)
    repos_path = f'{repos_path}_{str(datetime.now().microsecond)}/'
    click.echo("repos_path = " + repos_path)
    click.echo("repos_url = " + repos_url)
    click.echo("branch = " + branch)
    identifier = str(uuid.uuid4())
    with open(workflow_definition, 'r') as f:
        jobs_json = json.loads(f.read() % {"repos_path": repos_path, "dockerhub_password": dockerhub_password})

    print(f"Importing source files from Glow repo")
    run_cli_cmd(cli_profile, 'workspace', ['mkdirs', repos_path])
    run_cli_cmd(cli_profile, 'repos', ['create', '--url', repos_url, '--provider', 'gitHub', '--path', repos_path + "glow"])
    run_cli_cmd(cli_profile, 'repos', ['update', '--branch', branch, '--path', repos_path + "glow"])
    
    print(f"Create job")
    job_create = run_cli_cmd(cli_profile, 'jobs', ['create', '--json', json.dumps(jobs_json)])
    job_id = str(json.loads(job_create)['job_id'])
    print(f"Run job")
    job_run = run_cli_cmd(cli_profile, 'jobs', ['run-now', '--job-id', job_id])
    run_id = str(json.loads(job_run)['run_id'])
    print(f"Check job status")
    run_get = run_cli_cmd(cli_profile, 'runs', ['get', '--run-id', run_id])
    run_info = json.loads(run_get)
    while True:
        run_get = run_cli_cmd(cli_profile, 'runs', ['get', '--run-id', run_id])
        run_info = json.loads(run_get)
        print(f"=== Status check at {datetime.now().strftime('%H:%M:%S')} ===")
        base_msg = f"(Run ID [{run_info['state']['life_cycle_state']}]"
        if run_info['state']['life_cycle_state'] == 'INTERNAL_ERROR':
           print(base_msg, run_info['state']['result_state'], run_info['run_page_url'], ")")
           print("====================================")
           print("|  Exiting due to internal error.  |")
           print("====================================")
           run_cli_cmd(cli_profile, 'repos', ['delete', '--path', repos_path + "glow"])
           sys.exit(1)
        if run_info['state']['life_cycle_state'] == 'TERMINATED':
            print(base_msg, run_info['state']['result_state'], run_info['run_page_url'], ")")
            if run_info['state']['result_state'] == 'FAILED':
                print("===========================")
                print("|    Some tasks failed.    |")
                print("============================")
                run_cli_cmd(cli_profile, 'repos', ['delete', '--path', repos_path + "glow"])
                sys.exit(1)
            else:
                if run_info['state']['result_state'] == 'SUCCESS':
                    print("===========================")
                    print("|   All tasks succeeded!   |")
                    print("============================")
                    run_cli_cmd(cli_profile, 'repos', ['delete', '--path', repos_path + "glow"])
                    sys.exit(0)
        else:
            print(base_msg, run_info['state']['state_message'], ")")
        time.sleep(60)

if __name__ == '__main__':
    main()
