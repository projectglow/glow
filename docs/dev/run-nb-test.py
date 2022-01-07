'''
Runs all Glow notebooks in Databricks as an integration test.

Before running this, configure your Databricks CLI profile.

Example usage:
  python3 docs/dev/run-nb-test.py --cli-profile docs-ci --nbs etl/merge-vcf --nbs tertiary/pandas-lmm
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

JOBS_JSON = 'docs/dev/jobs-config.json'
NOTEBOOK_JOBS_JSON_MAPPING = 'docs/dev/notebook-jobs-config-mapping.json'

def run_cli_cmd(cli_profile, api, args):
    cmd = ['databricks', '--profile', cli_profile, api] + args
    res = subprocess.run(cmd, capture_output=True)
    if res.returncode != 0:
        raise ValueError(res)
    return res.stdout

def get_jobs_config(d, key, jobs_path="docs/dev/jobs-config.json"):
    """
    :param d: dictionary with mapping of notebooks to databricks jobs configuration (from NOTEBOOK_JOBS_JSON_MAPPING)
    :param key: notebook (nb) name
    :jobs_path: path to default jobs configuration to test notebooks
    """
    if key in d:
         jobs_path = d[key] 
    print("running notebook " + key + " with the following jobs configuration json " + jobs_path)
    return jobs_path

@click.command()
@click.option('--cli-profile', default='DEFAULT', help='Databricks CLI profile name.')
@click.option('--workspace-tmp-dir', default='/tmp/glow-nb-test-ci', help='Base workspace dir for import and testing.')
@click.option('--source-dir', default='docs/source/_static/zzz_GENERATED_NOTEBOOK_SOURCE',
              help='Source directory of notebooks to upload.')
@click.option('--nbs', multiple=True, default=[],
              help='Relative name of notebooks in the source directory to run. If not provided, runs all notebooks.')
def main(cli_profile, workspace_tmp_dir, source_dir, nbs):
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
        print(f"Importing source files from {source_dir} to {work_dir}")
        run_cli_cmd(cli_profile, 'workspace', ['mkdirs', work_dir])
        run_cli_cmd(cli_profile, 'workspace', ['import_dir', source_dir, work_dir])

        print(f"Launching runs")
        for nb in nbs:
            jobs_json_path = get_jobs_config(notebook_jobs_json_mapping, nb)
            with open(jobs_json_path, 'r') as f:
                jobs_json = json.load(f)
            jobs_json['name'] = 'Glow notebook integration test - ' + nb
            jobs_json['notebook_task'] = {'notebook_path': work_dir + '/' + nb}
            run_submit = run_cli_cmd(cli_profile, 'runs', ['submit', '--json', json.dumps(jobs_json)])
            run_id = json.loads(run_submit)['run_id']
            nb_to_run_id[nb] = str(run_id)
            while True:
                print(f"=== Status check at {datetime.now().strftime('%H:%M:%S')} ===")
                for nb, run_id in nb_to_run_id.items():
                    run_get = run_cli_cmd(cli_profile, 'runs', ['get', '--run-id', run_id])
                    run_info = json.loads(run_get)
                    nb_to_run_info[nb] = run_info
                for nb, run_info in nb_to_run_info.items():
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
            run_cli_cmd(cli_profile, 'workspace', ['rm', '-r', work_dir])
            sys.exit(0)

if __name__ == '__main__':
    main()
