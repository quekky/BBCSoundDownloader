import csv
import encodings.idna  # avoid encoding error in distributable
import os
import re
import shutil
import sys
import urllib.request
from multiprocessing import RLock
from multiprocessing.pool import ThreadPool
from pathlib import Path
from tqdm import tqdm

THREAD_COUNT = 10
MAX_FILENAME_LENGTH = 255  # this is the max with ecryptfs, but most systems are 255 chars max


class Downloader:
    def __init__(self, thread_count=THREAD_COUNT):
        self.thread_count = thread_count
        self.samples = self.get_samples()
        self.total_count = len(self.samples)
        self.finished = 0
        self.failed = 0

    def download_all(self):
        print('Downloading %s samples' % self.total_count)
        results = tqdm(ThreadPool(self.thread_count, initializer=tqdm.set_lock, initargs=(RLock(),)).imap_unordered(self.download, self.samples), total=self.total_count)
        failed_str = ''
        for success, filepath, e in results:
            if not success:
                failed_str += "%s failed with exception: %s\n" % (filepath, e)
        print(failed_str)
        print('%d failures reported.' % self.failed)

    def download(self, sample):
        url, filepath = sample
        try:
            filepath.parent.mkdir(parents=True, exist_ok=True)
            temp_path, headers = urllib.request.urlretrieve(url)
            shutil.move(temp_path, str(filepath))
            self.finished += 1
            tqdm.write('Finished %s' % (str(filepath)))
            return True, None, None
        except Exception as e:
            self.failed += 1
            print('FAILED ' + str(filepath), file=sys.stderr)
            print(e, file=sys.stderr)
            print('%d failed download attempts' % self.failed, file=sys.stderr)
            return False, filepath, e

    def get_samples(self):
        samples = []
        csv_path = os.path.join(os.path.dirname(__file__), 'BBCSoundEffects.csv')
        if not os.path.exists(csv_path):
            temp_path, headers = urllib.request.urlretrieve('http://bbcsfx.acropolis.org.uk/assets/BBCSoundEffects.csv')
            shutil.move(temp_path, csv_path)
        with open(csv_path, encoding='utf8') as f:
            reader = csv.DictReader(f)
            row_count = sum(1 for row in reader)
        with open(csv_path, encoding='utf8') as f:
            reader = csv.DictReader(f)
            print('Reading csv and checking files on disk')
            for row in tqdm(reader, total=row_count):
                folder = self.sanitize_path(row['CDName'])
                suffix = ' - ' + row['location']
                max_description_length = MAX_FILENAME_LENGTH - len(suffix)
                filename = self.sanitize_path(row['description'])[:max_description_length] + suffix
                filepath = Path('sounds') / folder / filename
                if not filepath.exists():
                    url = 'http://bbcsfx.acropolis.org.uk/assets/' + row['location']
                    samples.append((url, filepath))
        return samples

    def sanitize_path(self, path):
        return re.sub(r'[^\w\-&,()\. ]', '_', path).strip().strip('.')


if __name__ == "__main__":
    Downloader().download_all()
