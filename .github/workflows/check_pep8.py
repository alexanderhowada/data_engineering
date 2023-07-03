import os
import subprocess
import pycodestyle

def bash_command(command: str) -> str:
    """Execute a bash command and returns its output."""

    try:
        output = subprocess.check_output(command, shell=True, text=True)
        return output.strip()
    except subprocess.CalledProcessError as e:
        print(f"Command execution failed with return code {e.returncode}")
        return None
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

PATH = os.getcwd() + "/../../"

if __name__ == "__main__":
    # Execute command and get modified and added files.
    command = 'git diff --name-status weather_etl | grep -E "^[AM]\s+[a-zA-Z_/ \t]+\.py$"'
    files = bash_command(command)
    files = files.split("\n")
    files = [PATH+f.strip("AM \t") for f in files]

    # Create a StyleGuide instance and execute it
    style = pycodestyle.StyleGuide(
        max_line_length=120,
        ignore=["W292"]
    )
    results = style.check_files(files)

    print(f"file results: {results.get_file_results()}")
    print(f"total errors: {results.get_count()}")

    if results.get_count() > 0:
        raise Exception("PR does not comply with PEP 8.")