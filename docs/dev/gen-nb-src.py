'''
Transforms a .html notebook into its source .py/.scala/.r/,sql file.

This script is used by the CircleCI job 'check-docs'. Before running this, configure
your Databricks CLI profile.

Example usage:
  python3 docs/dev/gen-nb-src.py \
    --html docs/source/_static/notebooks/etl/variant-data.html
'''
import click
import subprocess
import os
import uuid

NOTEBOOK_DIR = 'docs/source/_static/notebooks'
SOURCE_DIR = 'docs/source/_static/zzz_GENERATED_NOTEBOOK_SOURCE'
SOURCE_EXTS = ['scala', 'py', 'r', 'sql']


@click.command()
@click.option('--html', required=True, help='Path of the HTML notebook.')
@click.option('--cli-profile', default='docs-ci', help='Databricks CLI profile name.')
@click.option('--workspace-tmp-dir', default='/Shared/glow-docs-ci', help='Temp workspace dir for import/export.')
def main(html, cli_profile, workspace_tmp_dir):
    assert os.path.commonpath([NOTEBOOK_DIR, html]) == NOTEBOOK_DIR, \
        "HTML notebook must be under {} but got {}.".format(NOTEBOOK_DIR, html)
    rel_path = os.path.splitext(os.path.relpath(html, NOTEBOOK_DIR))[0]

    if not os.path.exists(html):  # html notebook was deleted
        print("{} does not exist. Deleting the companion source file...".format(html))
        for ext in SOURCE_EXTS:
            source_path = os.path.join(SOURCE_DIR, rel_path + "." + ext)
            if os.path.exists(source_path):
                os.remove(source_path)
                print("\tDeleted {}.".format(source_path))
        return

    print("Generating source file for {} under {} ...".format(html, SOURCE_DIR))

    def run_cli_workspace_cmd(args):
        cmd = ['databricks', '--profile', cli_profile, 'workspace'] + args
        subprocess.run(cmd, capture_output=True)
        subprocess.check_output(cmd, stderr=subprocess.STDOUT)

    work_dir = os.path.join(workspace_tmp_dir, str(uuid.uuid4()))
    workspace_path = os.path.join(work_dir, rel_path)

    run_cli_workspace_cmd(['mkdirs', os.path.join(work_dir, os.path.dirname(rel_path))])
    try:
        # `-l PYTHON` is required by CLI but ignored with `-f HTML`
        # This command works for all languages in SOURCE_EXTS
        run_cli_workspace_cmd(['import', '-o', '-l', 'PYTHON', '-f', 'HTML', html, workspace_path])
        run_cli_workspace_cmd(['export_dir', '-o', work_dir, SOURCE_DIR])
    finally:
        run_cli_workspace_cmd(['rm', '-r', work_dir])


if __name__ == '__main__':
    main()
