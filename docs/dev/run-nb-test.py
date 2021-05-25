'''
Runs all Glow notebooks in Databricks as an integration test.

Before running this, configure your Databricks CLI profile.

Example usage:
  python3 docs/dev/run-nb-test.py
'''
import click
from datetime import datetime
import glob
import json
import subprocess
import time
import os
import uuid

SOURCE_DIR = 'docs/source/_static/zzz_GENERATED_NOTEBOOK_SOURCE'
CLUSTER_JSON = 'docs/dev/cluster-config.json'
SOURCE_EXTS = ['scala', 'py', 'r', 'sql']


def run_cli_cmd(cli_profile, api, args):
    cmd = ['databricks', '--profile', cli_profile, api] + args
    res = subprocess.run(cmd, capture_output=True)
    if res.returncode is not 0:
        raise ValueError(res)
    return res.stdout


@click.command()
@click.option('--cli-profile', default='DEFAULT', help='Databricks CLI profile name.')
@click.option('--workspace-tmp-dir', default='/tmp/glow-nb-test-ci', help='Base workspace dir for import and testing.')
def main(cli_profile, workspace_tmp_dir):
    identifier = str(uuid.uuid4())
    work_dir = os.path.join(workspace_tmp_dir, identifier)
    with open('docs/dev/jobs-config.json', 'r') as f:
        jobs_json = json.load(f)

    nbs = [os.path.relpath(path, SOURCE_DIR).split('.')[0]
           for path in glob.glob(SOURCE_DIR + '/**', recursive=True)
           if not os.path.isdir(path)]
    nb_to_run_id = {}

    try:
        print(f"Importing source files from {SOURCE_DIR} to {work_dir}")
        run_cli_cmd(cli_profile, 'workspace', ['mkdirs', work_dir])
        run_cli_cmd(cli_profile, 'workspace', ['import_dir', SOURCE_DIR, work_dir])

        for nb in nbs:
            jobs_json['name'] = 'Glow notebook integration test - ' + nb
            jobs_json['notebook_task'] = {'notebook_path': work_dir + '/' + nb}
            run_submit = run_cli_cmd(cli_profile, 'runs', ['submit', '--json', json.dumps(jobs_json)])
            run_id = json.loads(run_submit)['run_id']
            print(f"Launched run for notebook {nb} with run ID {run_id}")
            nb_to_run_id[nb] = str(run_id)
    finally:
        nb_to_run_state = {}
        while True:
            print(f"=== Status check at {datetime.now().strftime('%H:%M:%S')} ===")
            for nb, run_id in nb_to_run_id.items():
                run_get = run_cli_cmd(cli_profile, 'runs', ['get', '--run-id', run_id])
                run_state = json.loads(run_get)['state']
                nb_to_run_state[nb] = run_state
            if all([run_state['life_cycle_state'] == 'TERMINATED' for run_state in nb_to_run_state.values()]):
                for nb, run_state in nb_to_run_state.items():
                    print(f"{nb}: {run_state['result_state']}")
                break
            else:
                for nb, run_state in nb_to_run_state.items():
                    print(f"{nb} [{run_state['life_cycle_state']}] {run_state['state_message']}")
            time.sleep(60)
        run_cli_workspace_cmd(cli_profile, ['rm', '-r', work_dir])


if __name__ == '__main__':
    main()
