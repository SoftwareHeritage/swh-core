# Copyright (C) 2025 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

import concurrent
import logging
from pathlib import Path
import threading
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

if TYPE_CHECKING:
    from types_boto3_s3.service_resource import ObjectSummary

CHUNK_SIZE = 102400

logger = logging.getLogger(__name__)


class S3Downloader:
    """Utility class to recursively download the content of a directory on S3.

    It also implements a download resumption feature in case some files fail to
    be downloaded (when connection errors happen for instance).

    Args:
        local_path: path of directory where files will be downloaded
        s3_url: URL of directory in a S3 bucket (``s3://<bucket_name>/<path>/``)
        parallelism: maximum number of threads for downloading files

    Example of use::

        from swh.core.s3.downloader import S3Downloader

        # download "2025-05-18-popular-1k" datasets (ORC and compressed graph)
        # into a sub-directory of the current working directory named "2025-05-18-popular-1k"

        s3_downloader = S3Downloader(
            local_path="2025-05-18-popular-1k",
            s3_url="s3://softareheritage/graph/2025-05-18-popular-1k/",
        )

        while not s3_downloader.download():
            continue
    """

    def __init__(
        self,
        local_path: Path,
        s3_url: str,
        parallelism: int = 5,
    ) -> None:

        if not s3_url.startswith("s3://"):
            raise ValueError("Unsupported S3 URL")

        import boto3
        import botocore
        from botocore.handlers import disable_signing

        # silence noisy debug logs we are not interested about
        for module in ("boto3", "botocore", "s3transfer", "urllib3"):
            logging.getLogger(module).setLevel(logging.WARNING)

        self.s3 = boto3.resource("s3")
        # don't require credentials to list the bucket
        self.s3.meta.client.meta.events.register("choose-signer.s3.*", disable_signing)
        self.client = boto3.client(
            "s3",
            config=botocore.client.Config(
                # https://github.com/boto/botocore/issues/619
                max_pool_connections=10 * parallelism,
                # don't require credentials to download files
                signature_version=botocore.UNSIGNED,
            ),
        )

        self.local_path = local_path
        self.s3_url = s3_url
        self.bucket_name, self.prefix = self.s3_url[len("s3://") :].split("/", 1)
        self.parallelism = parallelism

        self.bucket = self.s3.Bucket(self.bucket_name)

    def _download_file(
        self,
        obj_key: str,
        # threading event to gracefully terminate thread when a download failed
        shutdown_event: Optional[threading.Event] = None,
        local_file_path: Optional[Path] = None,
        prefix: Optional[str] = None,
    ) -> str:

        prefix = prefix or self.prefix
        assert obj_key.startswith(prefix)
        relative_path = obj_key.removeprefix(prefix).lstrip("/")

        if local_file_path is None:
            local_file_path = self.local_path / relative_path

        local_file_path.parent.mkdir(parents=True, exist_ok=True)

        # fetch size of object to download
        object_metadata = self.client.head_object(Bucket=self.bucket_name, Key=obj_key)
        file_size = object_metadata["ContentLength"]

        file_part_path = Path(str(local_file_path) + ".part")

        if local_file_path.exists():
            # file already downloaded, we check if it has the correct size and
            # trigger a new download if it is not
            local_file_size = local_file_path.stat().st_size
            if local_file_size != file_size:
                logger.debug(
                    "File %s exists but has incorrect size, forcing a new download",
                    obj_key,
                )
                local_file_path.unlink()
                return self._download_file(
                    obj_key, shutdown_event, local_file_path, prefix
                )

            logger.debug("File %s already downloaded, nothing to do", obj_key)

        # download or resume download of a file
        elif self.can_download_file(relative_path, local_file_path):
            get_object_kwargs: Dict[str, Any] = {
                "Bucket": self.bucket_name,
                "Key": obj_key,
            }
            if file_part_path.exists():
                # resume previous download that failed by fetching only the missing bytes
                logger.debug("File %s was partially downloaded", obj_key)
                file_part_size = file_part_path.stat().st_size
                logger.debug(
                    "Resuming download of %s from byte %s",
                    obj_key,
                    file_part_size,
                )
                range_ = f"bytes={file_part_size}-{file_size}"
                assert file_part_size <= file_size, f"range {range_} is invalid"
                get_object_kwargs["Range"] = range_
            else:
                file_part_size = 0
                logger.debug("Downloading file %s", obj_key)

            if file_part_size < file_size:
                with file_part_path.open("ab") as file_part:
                    object_ = self.client.get_object(**get_object_kwargs)
                    for chunk in object_["Body"].iter_chunks(CHUNK_SIZE):
                        file_part.write(chunk)
                        if shutdown_event and shutdown_event.is_set():
                            # some files failed to be downloaded so abort current download to
                            # save state and inform user early by returning False in
                            # download_graph method
                            file_part.flush()
                            break

            if file_part_path.stat().st_size == file_size:
                # file fully downloaded, rename it
                file_part_path.rename(local_file_path)
                logger.debug("Downloaded file %s", obj_key)
                self.post_download_file(relative_path, local_file_path)

        else:
            logger.debug(
                "File %s already downloaded and uncompressed, nothing to do",
                obj_key,
            )

        return relative_path

    def download(
        self,
        progress_percent_cb: Callable[[int], None] = lambda _: None,
        progress_status_cb: Callable[[str], None] = lambda _: None,
    ) -> bool:
        """Execute the download of files from S3 in parallel using a pool of threads.

        Args:
            progress_percent_cb: Optional callback function to report the overall
                progress of the downloads
             progress_status_cb: Optional callback function to get status messages
                related to downloaded files

        Returns:
            :const:`True` if all files were successfully downloaded, :const:`False`
            if an error occurred while downloading a file, in that case calling that
            method again will resume such incomplete downloads

        """
        import tqdm

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.parallelism)
        shutdown_event = threading.Event()

        try:
            # recursively copy local files to S3
            objects = list(self.bucket.objects.filter(Prefix=self.prefix))
            with tqdm.tqdm(total=len(objects), desc="Downloading") as progress:
                not_done = futures = {
                    executor.submit(self._download_file, obj.key, shutdown_event)
                    for obj in self.filter_objects(objects)
                }
                while not_done:
                    # poll future states every second in order to abort downloads
                    # on first detected error
                    done, not_done = concurrent.futures.wait(
                        not_done,
                        timeout=1,
                        return_when=concurrent.futures.FIRST_COMPLETED,
                    )
                    for future in done:
                        progress.update()
                        progress_percent_cb(int(progress.n * 100 / progress.total))
                        progress_status_cb(f"Downloaded {future.result()}")

                concurrent.futures.wait(futures)

            self.post_downloads()

        except BaseException:
            logger.exception("Error occurred while downloading")
            # notify download threads to immediately terminate
            shutdown_event.set()
            # shutdown pool of download threads
            executor.shutdown(cancel_futures=True)
            # iterate on downloaded files to log partial ones
            for relative_path in self.local_path.rglob("**/*"):
                if relative_path.match("*.part"):
                    logger.debug(
                        "Downloaded bytes for %s dumped in %s to resume download later",
                        relative_path.name[:-5],
                        relative_path,
                    )
            return False
        return True

    def filter_objects(self, objects: List[ObjectSummary]) -> List[ObjectSummary]:
        """Method that can be overridden in derived classes to filter files to download,
        return all files by default.

        Args:
            objects: list of files recursively discovered from the S3 directory

        Returns:
            filtered list of files to download
        """
        return objects

    def can_download_file(self, relative_path: str, local_file_path: Path) -> bool:
        """Method that can be overridden in derived classes to prevent download of
        a file under certain conditions, download all files by default.

        Args:
            relative_path: path of file relative to the S3 directory
            local_file_path: local path where the file is downloaded

        Returns:
            whether to download the file or not
        """
        return True

    def post_download_file(self, relative_path: str, local_file_path: Path) -> None:
        """Method that can be overridden in derived classes to execute a post processing
        on a downloaded file (uncompress it for instance).

        Args:
            relative_path: path of file relative to the S3 directory
            local_file_path: local path where the file is downloaded
        """
        pass

    def post_downloads(self) -> None:
        """Method that can be overridden in derived classes to execute a post processing
        after all files were downloaded."""
        pass
