'''
Runs all Glow notebooks as an integration test in Databricks using 
1. Databricks CLI to set up continuous integration
  - Repos to sync notebooks from the Glow repository to your workspace https://docs.databricks.com/dev-tools/cli/repos-cli.html#repos-cli
  - multitask jobs to define the pipeline and run using Databricks Jobs API 2.1

Before running, configure your Databricks CLI profile https://docs.databricks.com/dev-tools/cli/index.html#set-up-authentication 
Examples of this can be found in the circleci configuration in the top level of the glow repo at `.circleci/config.yml`

Example usage:
  python3 docs/dev/run-nb-test.py --cli-profile <databricks cli profile> --json <configuration json>
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

JOBS_JSON = 'docs/dev/multitask-integration-test-config.json'

def run_cli_cmd(cli_profile, api, args):
    cmd = ['databricks', '--profile', cli_profile, api] + args
    res = subprocess.run(cmd, capture_output=True)
    if res.returncode != 0:
        raise ValueError(res)
    return res.stdout


@click.command()
@click.option('--cli-profile', default='DEFAULT', help='Databricks CLI profile name.')
@click.option('--repos-path', default='/Repos/staging/', help='Path in Databricks workspace for running integration test')
@click.option('--workspace-tmp-dir', default='/tmp/glow-nb-test-ci', help='Base workspace dir for import and testing.')
@click.option('--source-dir', default='docs/source/_static/zzz_GENERATED_NOTEBOOK_SOURCE',
              help='Source directory of notebooks to upload.')
@click.option('--nbs', multiple=True, default=[],
              help='Relative name of notebooks in the source directory to run. If not provided, runs all notebooks.')
def main(cli_profile,repos_path, workspace_tmp_dir, source_dir, nbs):
    identifier = str(uuid.uuid4())
    work_dir = os.path.join(workspace_tmp_dir, identifier)
    with open(JOBS_JSON, 'r') as f:
        jobs_json = json.load(f)
    with open(NOTEBOOK_JOBS_JSON_MAPPING, 'r') as f:
        notebook_jobs_json_mapping = json.load(f)

    if not nbs:
        nbs = [os.path.relpath(path, source_dir).split('.')[0]
               for path in glob.glob(source_dir + '/**', recursive=True)
               if not os.path.isdir(path)]
    nb_to_run_id = {}
    nb_to_run_info = {}

    try:
        print(f"Importing source files from Glow repo")
        run_cli_cmd(cli_profile, 'workspace', ['mkdirs', repos_path])
        run_cli_cmd(cli_profile, 'repos', ['create', '--url', 'https://github.com/projectglow/glow', '--provider', 'gitHub', '--path', repos_path + "glow"])

        print(f"Launching runs")
        jobs_json_path = get_jobs_config(notebook_jobs_json_mapping, nb)
        with open(jobs_json_path, 'r') as f:
            jobs_json = json.load(f)
        job_create = run_cli_cmd(cli_profile, 'jobs', ['create', '--json-file', json.dumps(jobs_json)])
        job_id = json.loads(job_create)['job_id']
        job_run = run_cli_cmd(cli_profile, 'jobs', ['run-now', '--job-id', job_id)
        run_id = json.loads(job_run)['run_id']
        while True:
            print(f"=== Status check at {datetime.now().strftime('%H:%M:%S')} ===")
            run_get = run_cli_cmd(cli_profile, 'runs', ['get', '--run-id', run_id])
            run_info = json.loads(run_get)
            base_msg = f"{nb} (Run ID {nb_to_run_id[nb]}) [{run_info['state']['life_cycle_state']}]"
            if run_info['state']['life_cycle_state'] == 'INTERNAL_ERROR':
               print(base_msg, run_info['state']['result_state'], run_info['run_page_url'])
               print("====================================")
               print("|  Exiting due to internal error.  |")
               print("====================================")
               sys.exit(1)
            if run_info['state']['life_cycle_state'] == 'TERMINATED':
                print(base_msg, run_info['state']['result_state'], run_info['run_page_url'])
                if run_info['state']['result_state'] == 'FAILED':
                    print("===========================")
                    print("|    Some tasks failed.    |")
                    print("============================")
                    sys.exit(1)
                else:
                    print(base_msg, run_info['state']['result_state'])
            else:
                print(base_msg, run_info['state']['state_message'])
            if all([run_info['state']['life_cycle_state'] == 'TERMINATED' for run_info in nb_to_run_info.values()]):
                break
            time.sleep(60)
    finally:
        if all([run_info['state']['result_state'] == 'SUCCESS' for run_info in nb_to_run_info.values()]):
            print("===========================")
            print("|   All tasks succeeded!   |")
            print("============================")
            run_cli_cmd(cli_profile, 'repos', ['delete', '--path', repos_path + "glow"])
            sys.exit(0)

if __name__ == '__main__':
    main()
