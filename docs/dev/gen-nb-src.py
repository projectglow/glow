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


def run_cli_workspace_cmd(cli_profile, args):
    cmd = ['databricks', '--profile', cli_profile, 'workspace'] + args
    res = subprocess.run(cmd, capture_output=True)
    if res.returncode != 0:
        raise ValueError(res)


@click.command()
@click.option('--html', required=True, help='Path of the HTML notebook.')
@click.option('--cli-profile', default='DEFAULT', help='Databricks CLI profile name.')
@click.option('--workspace-tmp-dir', default='/tmp/glow-docs-ci', help='Base workspace dir; a temporary directory will be generated under this for import/export.')
def main(html, cli_profile, workspace_tmp_dir):
    assert os.path.commonpath([NOTEBOOK_DIR, html]) == NOTEBOOK_DIR, \
        f"HTML notebook must be under {NOTEBOOK_DIR} but got {html}."
    rel_path = os.path.splitext(os.path.relpath(html, NOTEBOOK_DIR))[0]

    if not os.path.exists(html):  # html notebook was deleted
        print(f"{html} does not exist. Deleting the companion source file...")
        for ext in SOURCE_EXTS:
            source_path = os.path.join(SOURCE_DIR, rel_path + "." + ext)
            if os.path.exists(source_path):
                os.remove(source_path)
                print(f"\tDeleted {source_path}.")
        return

    print(f"Generating source file for {html} under {SOURCE_DIR} ...")

    work_dir = os.path.join(workspace_tmp_dir, str(uuid.uuid4()))
    workspace_path = os.path.join(work_dir, rel_path)

    run_cli_workspace_cmd(cli_profile, ['mkdirs', os.path.join(work_dir, os.path.dirname(rel_path))])
    try:
        # `-l PYTHON` is required by CLI but ignored with `-f HTML`
        # This command works for all languages in SOURCE_EXTS
        run_cli_workspace_cmd(cli_profile, ['import', '-o', '-l', 'PYTHON', '-f', 'HTML', html, workspace_path])
        run_cli_workspace_cmd(cli_profile, ['export_dir', '-o', work_dir, SOURCE_DIR])
    finally:
        run_cli_workspace_cmd(cli_profile, ['rm', '-r', work_dir])


if __name__ == '__main__':
    main()
