import os
from typing import Optional

import paramiko


def sftp_put_and_verify(
    host: str,
    port: int,
    username: str,
    key_path: str,
    local_path: str,
    remote_dir: str,
) -> str:
    """
    Upload a file via SFTP using an RSA key and verify by comparing sizes.

    Returns the full remote path on success.
    Raises an exception on any error or size mismatch.
    """
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Local file does not exist: {local_path}")

    key = paramiko.RSAKey.from_private_key_file(key_path)

    transport: Optional[paramiko.Transport] = None
    sftp: Optional[paramiko.SFTPClient] = None

    try:
        transport = paramiko.Transport((host, int(port)))
        transport.connect(username=username, pkey=key)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Ensure remote directory exists (best-effort)
        try:
            sftp.chdir(remote_dir)
        except IOError:
            # Attempt to create nested directories
            parts = [p for p in remote_dir.split("/") if p]
            path_so_far = ""
            for part in parts:
                path_so_far = f"{path_so_far}/{part}" if path_so_far else part
                try:
                    sftp.chdir(path_so_far)
                except IOError:
                    sftp.mkdir(path_so_far)
                    sftp.chdir(path_so_far)

        filename = os.path.basename(local_path)
        remote_path = f"{remote_dir.rstrip('/')}/{filename}"

        sftp.put(local_path, remote_path)

        local_size = os.path.getsize(local_path)
        remote_stat = sftp.stat(remote_path)
        if local_size != remote_stat.st_size:
            raise IOError(
                f"Size mismatch after upload: local={local_size}, remote={remote_stat.st_size}"
            )

        return remote_path
    finally:
        if sftp is not None:
            sftp.close()
        if transport is not None:
            transport.close()

