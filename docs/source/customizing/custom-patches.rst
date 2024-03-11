.. meta::
  :description: Customizing the Databricks environment


==========================================
Maintaining private patches on top of Glow
==========================================

Some organizations wish to maintain forks of Glow with some private patches for non-standard configuration or during feature development. 
For this pattern, we recommend the following git workflow. We assume that ``oss`` refers to the `open source Glow repository <https://github.com/projectglow/glow>_`
and ``origin`` refers to your fork.

1. Set up the initial branch

If you use Github, you can use the fork button to start your repository. In a pure git workflow, you can clone the open source repository

.. code-block:: sh
    
    git clone --origin oss git@github.com:projectglow/glow.git
    git remote add origin <your-repository-url>

2. Make changes

You can use whatever workflow you want, for example merging pull requests or pushing directly.

3. Squash changes before pulling open source changes

Before pulling open source changes, we recommend squashing the private patches to simplify managing merge conflicts.

.. code-block:: sh

    git fetch oss main
    git reset --soft $(git merge-base HEAD oss/main)
    git commit --edit -m"$(git log --format=%B --reverse HEAD..HEAD@{1})"

4. Rebase and push

Note that since git history has been rewritten, you must force push to the remote repository. You may want to back up changes in a separate branch before proceeding.

.. code-block:: sh

    git rebase oss/main
    git push -f origin
