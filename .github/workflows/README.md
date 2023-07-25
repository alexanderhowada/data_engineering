# CI using GitHub actions.

## Description

This folder implements CI using GitHub actions.
In the future, I will also add CD as a second GitHub action.

## CI

The CI is defined by the <code>ci.yml</code> file, and executed when creating a PR to the main branch.
The <code>ci.yml</code> executes the script <code>run.sh</code>, which calls <code>docker-compose</code> and manages the logs.
Each service defined in <code>docker-compose</code> executes a different CI procedure and outputs a log file.
The log file must be empty if there are no errors.
In the end, all log files are read.

### Check pep 8

The check PEP 8 procedure verifies if the new <code>*.py</code> codes follow the PEP 8 guidelines.
If it does not follow, the CI fails and outputs the errors.

### unittest

The <code>unittest</code> folder defines the procedure that discovers and executes unittests using the <code>unittest</code> Python library.
Note that folders missing <code>__init__.py</code> will not be executed.