import os
import signal
import subprocess
import socket
import time
import stat

from filelock import FileLock, Timeout


class MotoServer:
    """
    Handles the lifecycle of the moto s3 test stub server.
    """
    process = None
    pid = None
    port = None

    @classmethod
    def start(cls):
        # Remove lock file if it's too old and hanging around the server
        lock_file = "/tmp/start-moto.lock"
        if os.path.exists(lock_file):
            print(f"Lock file {lock_file} exists")
            age = cls._file_age_in_seconds(lock_file)
            print(f"Lock file is {age} seconds old")
            max_age = 15 * 60
            if age > max_age:
                print(f"Deleting existing lock file since older than {max_age} seconds")
                os.remove(lock_file)

        try:
            # Obtain a file lock to ensure only one moto server started at the same time on this server.
            print(f"Obtaining lock to start moto server")
            lock = FileLock(lock_file, timeout=2*60)
            with lock:
                # Choose a port to use
                print(f"Lock obtained, choosing port")
                cls._choose_moto_port()

                # Kill any existent moto server on port
                print(f"Port {cls.port} chosen, looking for existing moto server on port {cls.port}")
                pid = cls._get_existing_moto_server_pid(cls.port)
                if pid is not None:
                    print(f"Found existing moto server on port {cls.port}, killing")
                    os.kill(pid, signal.SIGTERM)
                    print("Killed")

                # Run moto server
                print(f"Running moto server on port {cls.port}")
                cls.process = subprocess.Popen(
                    f"moto_server --port {cls.port} s3",
                    stdout=subprocess.PIPE,
                    shell=True,
                    preexec_fn=os.setsid
                )
                if not cls._wait_for_port(cls.port):
                    raise RuntimeError(f"Could not connect to moto port {cls.port}")

        except Timeout:
            print("Timed out on waiting for moto lock")
            raise

    @classmethod
    def stop(cls):
        # Shut down moto server
        if cls.process is not None and cls.process.pid is not None:
            pid = cls.process.pid
            max_attempts = 10
            retry_delay = 2
            attempts = 1
            while cls._get_existing_moto_server_pid(cls.port) is not None:
                print(f"Killing moto server with pid {pid}")
                os.killpg(os.getpgid(pid), signal.SIGTERM)
                if attempts > max_attempts:
                    raise RuntimeError(f"Could not kill moto server with pid {pid} after {max_attempts} attempts")
                time.sleep(retry_delay)
                attempts += 1
            cls.process.pid = None

    @classmethod
    def _choose_moto_port(cls, port=5000, max_port=65535):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        while port <= max_port:
            try:
                sock.bind(("localhost", port))
                sock.close()
                cls.port = port
                return
            except OSError:
                pass
            port += 1
        raise IOError("No free ports")

    @classmethod
    def _wait_for_port(cls, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        max_timeout = 10
        sock.settimeout(max_timeout)
        for _ in range(max_timeout):
            time.sleep(1)
            result = sock.connect_ex(("localhost", int(port)))
            if result == 0:
                return True
        print(f"Could not connect to port {port} after {max_timeout}")
        return False

    @classmethod
    def _get_existing_moto_server_pid(cls, port):
        # Kill the moto server if it exists.
        # Checks the moto port and kills the process regardless of what it is.
        cmd = f"lost -t -i:{port}"
        c = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = c.communicate()
        if stdout != b'':
            pid = int(stdout.decode())
            return pid
        return None

    @classmethod
    def _file_age_in_seconds(cls, pathname):
        return time.time() - os.stat(pathname)[stat.ST_MTIME]
