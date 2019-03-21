import logging
import os
import io
import json
import gcv2hocr
import datetime
import pytz
import time
from PIL import Image
from shutil import copyfile
from dateutil import tz
from create_pdf import export_pdf
from google.cloud import storage
from google.cloud import vision
from google.protobuf.json_format import MessageToDict
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from tempfile import NamedTemporaryFile

CONFIG_BUCKET = "scanit-config"

logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

storage_client = storage.Client()
vision_client = vision.ImageAnnotatorClient()


class ScanitPath:
    @staticmethod
    def from_fn(bucket, fn):
        # fn: "<timestamp>/<nr>-<total>_<owners>.<ext>"
        directory = os.path.dirname(fn)
        (seq_info, filename) = os.path.basename(fn).split("_", 2)
        (owners, ext) = os.path.splitext(filename)
        (nr, total) = seq_info.split("-")
        return ScanitPath(bucket, directory, int(nr), int(total), owners)

    def __init__(self, bucketname, directory, nr, total, owners):
        self.bucketname = bucketname
        self.directory = directory
        self.nr = nr
        self.total = total
        if "." in owners:
            self.owners = set(owners.split("."))
        else:
            self.owners = set([owners])

    def gs_url(self, ext):
        return f"gs://{self.bucketname}/{self.full_path(ext)}"

    def full_path(self, ext):
        return f"{self.base_path}.{ext}"

    @property
    def base_path(self):
        owners = ".".join(self.owners)
        return f"{self.directory}/{self.nr}-{self.total}_{owners}"


def scanit(data, context):
    fn = data['name']
    bucket = data['bucket']
    logging.debug(f"Triggered by {fn}")

    if fn.endswith("jpg"):
        s = ScanitPath.from_fn(bucket, fn)
        ext = "jpg"
        logging.debug(f"OCRing {s.gs_url(ext)}")
        resp = vision_client.text_detection({
            'source': {'image_uri': s.gs_url(ext)}
        })

        annotations = resp.text_annotations

        if len(annotations) > 0:
            logging.debug(f"OCR for {s.gs_url(ext)} done")
            text = annotations[0].description

            s.owners = set(_get_owners(text))
            logging.debug(f"Found owners in {s.gs_url(ext)}: {s.owners}")

            contents = gcv2hocr.fromResponse(MessageToDict(resp)).render()
        else:
            contents = gcv2hocr.fromResponse(False).render()

        # # A race condition might happen here where between the creation
        # # and the _all_complete() call all hocr files have arrived.
        # # This causes multiple PDFS to be generated and uploaded.
        # # Since we don't have any synchornization mechanisms for cloud functions
        # # we wait here a little bit (nr-1 seconds)
        # logging.debug(f"Waiting {s.nr - 1} seconds before upload")
        # time.sleep(s.nr - 1)
        _store(s, "hocr", contents)

    if fn.endswith("hocr"):
        s = ScanitPath.from_fn(bucket, fn)
        ext = "hocr"

        if _all_complete(s, "hocr"):

            logging.debug(f"{s.full_path(ext)} completes the set.")
            hocrs = []
            owners = set()
            jpgs = list(map(lambda f: Image.open(io.BytesIO(
                f.download_as_string())), _all_files(s, "jpg")))

            for f in _all_files(s, "hocr"):
                sp = ScanitPath.from_fn(s.bucketname, f.name)
                logging.debug(f"Owners from {f.name}: {sp.owners}")
                owners.update(sp.owners)
                hocrs.append(f.download_as_string().decode("utf-8"))

            logging.debug(f"All owners: {owners}")

            logging.debug(f"Creating {len(jpgs)} page PDF")
            pdf = export_pdf(jpgs, hocrs, title="TEST")

            s.owners = owners
            _store(s, "pdf", pdf)
        else:
            logging.debug(f"Waiting for more files to complete {s.directory}")

    if fn.endswith("pdf"):
        s = ScanitPath.from_fn(bucket, fn)

        with NamedTemporaryFile(delete=False) as tempfile:
            tempfile.write(_get(s, "pdf"))
            tempfile.close()

            if len(s.owners) == 1 and "unknown" in s.owners:
                s.owners = {'wouter', 'sandy'}
            elif len(s.owners) > 1 and "unknown" in s.owners:
                s.owners.remove("unknown")

            title = _utc_to_local(s.directory).strftime(
                "Scanned Document - %a %d %b %Y at %H:%M")
            for owner in s.owners:
                logging.debug(f"GAuth {owner}")
                gauth = _auth(owner)
                drive = GoogleDrive(gauth)
                file_drive = drive.CreateFile({'title': f"{title}.pdf"})
                file_drive.SetContentFile(tempfile.name)
                file_drive.Upload()
                logging.debug(
                    f"{s.gs_url('pdf')} uploaded to {owner}'s Google Drive as '{title}.pdf'")


def _utc_to_local(t):
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('America/New_York')
    utc = datetime.datetime.fromtimestamp(int(t))
    utc = utc.replace(tzinfo=from_zone)
    return utc.astimezone(to_zone)


def _store(scanit_path, ext, contents):
    bucket = storage_client.get_bucket(scanit_path.bucketname)
    blob = bucket.blob(scanit_path.full_path(ext))
    blob.upload_from_string(contents)
    logging.debug(f"Stored {scanit_path.gs_url(ext)}")


OWNERS_MAP = {
    'sandy': {
        "terms": ['sandy', 'sandra', 'bounds', 'leiko']
    },
    'wouter': {
        "terms": ['wouter', 'wpm', 'w.p.m.', 'petrus', 'maria', 'bie']
    }
}


def _get_owners(text):
    owners = []
    text = text.lower()
    for owner, conf in OWNERS_MAP.items():
        for term in conf['terms']:
            if term in text:
                owners.append(owner)
    if len(owners) == 0:
        owners.append('unknown')

    logging.debug(f"Owners found in text: {owners}")
    return owners


def _get(scanit_path, ext):
    logging.debug(f"Downloading {scanit_path.full_path(ext)}..")
    bucket = storage_client.get_bucket(scanit_path.bucketname)
    blob = bucket.get_blob(scanit_path.full_path(ext))
    return blob.download_as_string()


def _all_files(scanit_path, ext=""):
    bucket = storage_client.get_bucket(scanit_path.bucketname)
    return [blob for blob in bucket.list_blobs(prefix=scanit_path.directory) if blob.name.endswith(ext)]


def _all_complete(scanit_path, ext=""):
    files = _all_files(scanit_path, ext)
    return len(files) == scanit_path.total


def _auth(owner):
    bucket = storage_client.get_bucket(CONFIG_BUCKET)
    f = NamedTemporaryFile(delete=False)
    f.write(bucket.get_blob(f"{owner}.json").download_as_string())
    f.close()

    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(f.name)
    if gauth.access_token_expired:
        gauth.Refresh()
        gauth.SaveCredentialsFile(f.name)
        logging.debug(f"Upload {owner}.json")
        bucket.blob(f"{owner}.txt").upload_from_filename(f.name)
    else:
        gauth.Authorize()

    return gauth
