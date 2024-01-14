import subprocess,os

def run_script(commands,bin_file="./db",db_file="",is_remove=False):
    if is_remove and os.path.exists(db_file):
        print(f"{db_file} exists, remove it first")
        os.remove(db_file) 

    raw_output = None
    with subprocess.Popen([bin_file, db_file], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
        for command in commands:
            try:
                process.stdin.write(command + '\n')
            except BrokenPipeError:
                break
        process.stdin.close()
        raw_output = process.stdout.read()
    return raw_output.splitlines()
