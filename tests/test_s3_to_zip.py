"""Потоковый ZIP S3→S3 (`plugins.s3_utils.s3_to_zip`) на заглушке клиента S3, без сети.

Прогон: `python -m pytest tests -q` в окружении Airflow 3 (`/opt/aftest/venv312`).
"""
import io
import os
import zipfile
from datetime import datetime, timezone

import pytest
from airflow.sdk.exceptions import AirflowFailException
from botocore.exceptions import ClientError

from plugins import s3_utils


class Body:
    def __init__(self, data, fail=False):
        self.data, self.fail = data, fail

    def iter_chunks(self, chunk_size):
        if self.fail:                   # кусок прочитан — и обрыв посреди объекта
            yield self.data[:10]
            raise IOError('обрыв чтения')
        for i in range(0, len(self.data), chunk_size):
            yield self.data[i:i + chunk_size]


class Client:
    def __init__(self, objs):
        self.objs = dict(objs)          # key -> (bytes, LastModified)
        self.parts, self.aborted, self.fail_key = {}, [], None

    def get_paginator(self, name):
        client = self

        class P:
            def paginate(self, Bucket, Prefix):
                if name == 'list_multipart_uploads':
                    yield {'Uploads': [{'Key': k, 'UploadId': k} for k in client.parts if k.startswith(Prefix)]}
                    return
                rows = [{'Key': k, 'Size': len(d), 'LastModified': t}
                        for k, (d, t) in sorted(client.objs.items()) if k.startswith(Prefix)]
                for i in range(0, len(rows), 1000):     # страницы, как у list_objects_v2
                    yield {'Contents': rows[i:i + 1000]}
        return P()

    def get_object(self, Bucket, Key):
        return {'Body': Body(self.objs[Key][0], fail=Key == self.fail_key)}

    def head_object(self, Bucket, Key):
        if Key not in self.objs:
            raise ClientError({'Error': {'Code': '404'}}, 'HeadObject')
        return {}

    def create_multipart_upload(self, Bucket, Key):
        self.parts[Key] = []
        return {'UploadId': Key}

    def upload_part(self, Bucket, Key, UploadId, PartNumber, Body):
        self.parts[Key].append(Body)
        return {'ETag': str(PartNumber)}

    def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):
        assert [p['PartNumber'] for p in MultipartUpload['Parts']] == list(range(1, len(self.parts[Key]) + 1))
        self.objs[Key] = (b''.join(self.parts.pop(Key)), datetime.now(timezone.utc))

    def abort_multipart_upload(self, Bucket, Key, UploadId):
        self.aborted.append(Key)
        self.parts.pop(Key)


D1, D2 = datetime(2026, 10, 1, 12, tzinfo=timezone.utc), datetime(2026, 10, 5, 12, tzinfo=timezone.utc)
BIG = os.urandom(300_000)               # больше части: архив уходит несколькими частями


@pytest.fixture
def cl(monkeypatch):
    c = Client({'logs/a/1.log': (b'x' * 1000, D1), 'logs/a/2.gz': (BIG, D2),
                'logs/b/3.log': (b'y' * 10, D2), 'logs/dir/': (b'', D1), 'other/4.log': (b'z', D1),
                'logs/пусто.log': (b'', D1)})
    monkeypatch.setattr(s3_utils, 'S3Hook', lambda aws_conn_id: type('H', (), {'get_conn': lambda self: c})())
    return c


def unzip(c, key):
    with zipfile.ZipFile(io.BytesIO(c.objs[key][0])) as z:
        assert z.testzip() is None
        return {i.filename: z.read(i) for i in z.infolist()}


def test_roundtrip_prefix_multipart(cl):
    r = s3_utils.s3_to_zip('s3://b/logs', 's3://b/arc/x.zip', part_size=100_000)
    assert unzip(cl, 'arc/x.zip') == {'a/1.log': b'x' * 1000, 'a/2.gz': BIG, 'b/3.log': b'y' * 10, 'пусто.log': b''}
    assert (r['path'], r['count'], r['raw']) == ('s3://b/arc/x.zip', 4, 1000 + len(BIG) + 10)
    assert len(cl.objs['arc/x.zip'][0]) == r['size'] and r['size'] > 100_000


def test_mask_dates_and_auto_name(cl):
    r = s3_utils.s3_to_zip('s3://b/logs/*/*.log', 's3://b/arc/', date_from='2026-10-05', date_to='2026-10-05')
    assert r['path'].startswith('s3://b/arc/logs_') and r['path'].endswith('.zip')
    assert unzip(cl, r['path'].split('/', 3)[3]) == {'b/3.log': b'y' * 10}


def test_empty_limit_exists_bad_date(cl):
    with pytest.raises(AirflowFailException, match='нечего'):
        s3_utils.s3_to_zip('s3://b/nope/', 's3://b/arc/')
    with pytest.raises(AirflowFailException, match='лимит'):
        s3_utils.s3_to_zip('s3://b/logs/', 's3://b/arc/', max_bytes=1000)
    s3_utils.s3_to_zip('s3://b/logs/', 's3://b/x.zip')
    with pytest.raises(AirflowFailException, match='уже есть'):
        s3_utils.s3_to_zip('s3://b/logs/', 's3://b/x.zip')
    with pytest.raises(AirflowFailException, match='дат'):
        s3_utils.s3_to_zip('s3://b/logs/', 's3://b/arc/', date_from='05.10.2026')
    s3_utils.s3_to_zip('s3://b/logs/', 's3://b/x.zip', replace=True)   # перезапись разрешена


def test_abort_on_failure(cl):
    cl.fail_key = 'logs/a/2.gz'
    with pytest.raises(IOError):
        s3_utils.s3_to_zip('s3://b/logs/', 's3://b/arc/y.zip', part_size=100_000)
    assert cl.aborted == ['arc/y.zip'] and 'arc/y.zip' not in cl.objs


def test_abort_stale_uploads(cl):
    cl.parts = {'arc/old.zip': [b'part'], 'arc/sub/keep.zip': [b'p'], 'arc/keep.csv': [b'p']}
    r = s3_utils.s3_to_zip('s3://b/logs/', 's3://b/arc/z.zip')
    assert cl.aborted == ['arc/old.zip'] and r['aborted'] == ['arc/old.zip']
    assert set(cl.parts) == {'arc/sub/keep.zip', 'arc/keep.csv'}


def test_zip64_many_entries(cl):
    """Больше 65 535 записей — нужен ZIP64-каталог; несколько страниц листинга."""
    cl.objs.update({f'many/{i:05}.log': (b'', D1) for i in range(66_000)})
    r = s3_utils.s3_to_zip('s3://b/many/', 's3://b/arc/many.zip')
    with zipfile.ZipFile(io.BytesIO(cl.objs['arc/many.zip'][0])) as z:
        assert len(z.infolist()) == r['count'] == 66_000
