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
import os
import subprocess
import sys
import time
import uuid

SOURCE_DIR = 'docs/source/_static/zzz_GENERATED_NOTEBOOK_SOURCE'
JOBS_JSON = 'docs/dev/jobs-config.json'
INIT_SCRIPT_DIR = 'docs/dev/init-scripts'


def run_cli_cmd(cli_profile, api, args):
    cmd = ['databricks', '--profile', cli_profile, api] + args
    res = subprocess.run(cmd, capture_output=True)
    if res.returncode is not 0:
        raise ValueError(res)
    return res.stdout


@click.command()
@click.option('--cli-profile', default='DEFAULT', help='Databricks CLI profile name.')
@click.option('--workspace-tmp-dir', default='/tmp/glow-nb-test-ci', help='Base workspace dir for import and testing.')
@click.option('--dbfs-init-script-dir', default='dbfs:/glow-init-scripts', help='DBFS directory for init scripts.')
def main(cli_profile, workspace_tmp_dir, dbfs_init_script_dir):
    identifier = str(uuid.uuid4())
    work_dir = os.path.join(workspace_tmp_dir, identifier)
    with open(JOBS_JSON, 'r') as f:
        jobs_json = json.load(f)

    nbs = [os.path.relpath(path, SOURCE_DIR).split('.')[0]
           for path in glob.glob(SOURCE_DIR + '/**', recursive=True)
           if not os.path.isdir(path)]
    nb_to_run_id = {}

    try:
        print(f"Importing source files from {SOURCE_DIR} to {work_dir}")
        run_cli_cmd(cli_profile, 'workspace', ['mkdirs', work_dir])
        run_cli_cmd(cli_profile, 'workspace', ['import_dir', SOURCE_DIR, work_dir])

        print(f"Installing init scripts")
        run_cli_cmd(cli_profile, 'fs', ['cp', INIT_SCRIPT_DIR, dbfs_init_script_dir, '--recursive', '--overwrite'])

        print(f"Launching runs")
        for nb in nbs:
            jobs_json['name'] = 'Glow notebook integration test - ' + nb
            jobs_json['notebook_task'] = {'notebook_path': work_dir + '/' + nb}
            run_submit = run_cli_cmd(cli_profile, 'runs', ['submit', '--json', json.dumps(jobs_json)])
            run_id = json.loads(run_submit)['run_id']
            nb_to_run_id[nb] = str(run_id)
    finally:
        nb_to_run_info = {}
        while True:
            print(f"=== Status check at {datetime.now().strftime('%H:%M:%S')} ===")
            for nb, run_id in nb_to_run_id.items():
                run_get = run_cli_cmd(cli_profile, 'runs', ['get', '--run-id', run_id])
                run_info = json.loads(run_get)
                nb_to_run_info[nb] = run_info
            for nb, run_info in nb_to_run_info.items():
                base_msg = f"{nb} (Run ID {nb_to_run_id[nb]}) [{run_info['state']['life_cycle_state']}]"
                if run_info['state']['life_cycle_state'] == 'TERMINATED':
                    if run_info['state']['result_state'] == 'FAILED':
                        print(base_msg, run_info['state']['result_state'], run_info['run_page_url'])
                    else:
                        print(base_msg, run_info['state']['result_state'])
                else:
                    print(base_msg, run_info['state']['state_message'])
            if all([run_info['state']['life_cycle_state'] == 'TERMINATED' for run_info in nb_to_run_info.values()]):
                break
            time.sleep(60)
        if all([run_info['state']['result_state'] == 'SUCCESS' for run_info in nb_to_run_info.values()]):
            print("===========================")
            print("|   All tasks succeeded!   |")
            print("============================")
            run_cli_cmd(cli_profile, 'workspace', ['rm', '-r', work_dir])
            sys.exit(0)
        else:
            print("===========================")
            print("|    Some tasks failed.    |")
            print("============================")
            sys.exit(1)


if __name__ == '__main__':
    main()
