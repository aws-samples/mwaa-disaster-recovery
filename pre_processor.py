import shutil
import errno
import config

class Preprocessor:
    def __init__(self, conf=None):
        self.conf = conf or config.Config()

    def process(self):
        mwaa_version = self.conf.get(config.MWAA_VERSION)
        schedule_interval = self.conf.get(config.PRIMARY_SCHEDULE_INTERVAL)
        self.copy_path(f'assets/dags/{mwaa_version}', f'build/dags')
        with open(f'build/dags/dr_config.py', 'w') as f:
            f.write(f'')


    def copy_path(self, source: str, destination: str):
        try:
            shutil.copytree(source, destination)
        except OSError as exc:
            if exc.errno in (errno.ENOTDIR, errno.EINVAL):
                shutil.copy(source, destination)
            else: raise
