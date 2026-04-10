import requests
import logging
from airflow.hooks.base import BaseHook

class WebHDFSHook(BaseHook):
    """
    Custom Hook to interact with WebHDFS API.
    """
    def __init__(self, webhdfs_conn_id='webhdfs_default', base_url="http://hdfs-namenode:9870/webhdfs/v1", user="root"):
        super().__init__()
        self.base_url = base_url
        self.user = user
        self.logger = logging.getLogger(__name__)

    def mkdirs(self, hdfs_path: str) -> bool:
        """
        Create directory in HDFS.
        """
        url = f"{self.base_url}{hdfs_path}?op=MKDIRS&user.name={self.user}"
        response = requests.put(url)
        if response.status_code == 200:
            self.logger.info(f"Successfully created directory: {hdfs_path}")
            return True
        else:
            self.logger.error(f"Failed to create directory {hdfs_path}: {response.text}")
            return False

    def create(self, hdfs_path: str, local_path: str) -> bool:
        """
        Upload file to HDFS using 2-step PUT (Step 1: Redirect, Step 2: Upload).
        """
        # Step 1: Create request to get redirect URL
        url = f"{self.base_url}{hdfs_path}?op=CREATE&user.name={self.user}&overwrite=true"
        response = requests.put(url, allow_redirects=False)
        
        if response.status_code == 307:
            redirect_url = response.headers['Location']
            # Step 2: Send actual data to the redirect URL
            with open(local_path, 'rb') as f:
                upload_response = requests.put(redirect_url, data=f)
                if upload_response.status_code == 201:
                    self.logger.info(f"Successfully uploaded {local_path} to {hdfs_path}")
                    return True
                else:
                    self.logger.error(f"Failed to upload data to {hdfs_path}: {upload_response.text}")
                    return False
        else:
            self.logger.error(f"Failed to initiate file creation for {hdfs_path}: {response.text}")
            return False

    def open(self, hdfs_path: str) -> bytes:
        """
        Read file content from HDFS.
        """
        url = f"{self.base_url}{hdfs_path}?op=OPEN&user.name={self.user}"
        response = requests.get(url, allow_redirects=True)
        if response.status_code == 200:
            return response.content
        else:
            self.logger.error(f"Failed to open file {hdfs_path}: {response.text}")
            raise Exception(f"Failed to open file {hdfs_path}")

    def exists(self, hdfs_path: str) -> bool:
        """
        Check if path exists in HDFS.
        """
        url = f"{self.base_url}{hdfs_path}?op=GETFILESTATUS&user.name={self.user}"
        response = requests.get(url)
        return response.status_code == 200

    def list_status(self, hdfs_path: str) -> list:
        """
        List status of files in a directory.
        """
        url = f"{self.base_url}{hdfs_path}?op=LISTSTATUS&user.name={self.user}"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()["FileStatuses"]["FileStatus"]
        else:
            self.logger.error(f"Failed to list status for {hdfs_path}: {response.text}")
            return []
