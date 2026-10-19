"""###🛠️ Утилиты S3 (`plugins/s3_utils.py`)
*2026-10-19 17:24 MSK · v1.9 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Расширенные функции для работы с S3.

| Функция | Описание |
|---|---|
| `get_s3_list()` | Ленивая инициализация списка бакетов по всем соединениям (кешируется) |
| `s3_set_ttl` / `s3_get_ttl` / `s3_del_ttl` | Управление жизненным циклом объектов |
| `s3_create_bucket` / `s3_get_buckets` / `s3_bucket_size` | Операции с бакетами |
| `s3_account_usage` / `s3_fill_check` | Занятое всеми бакетами учётки и вердикт заполнения к её квоте |
| `s3_path_parse` | Разбор `conn_id://bucket/prefix/mask` |
| `s3_to_s3` / `s3_move_s3` / `s3_done` | Копирование, перемещение, `.done`-файлы |
| `s3_from_zip` | Потоковое извлечение из ZIP без сохранения на диск |
| `s3_to_zip` | Потоковая сборка объектов по пути/маске/датам в один ZIP64 в S3 (своя multipart, отмена при сбое) |
| `s3_keys` | Список объектов по маске (`*`, `?`) |
| `s3_IterStream` / `s3_gzip_stream` | Потоковые обёртки для загрузки в S3 (защита от OOM: лимит 512 МБ) |

> Вызывайте `get_s3_list()` вместо обращения к модульной переменной `s3_list`.
"""

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
from stream_unzip import stream_unzip # type: ignore
from stream_zip import stream_zip, ZIP_AUTO  # type: ignore
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

# Фолбэк обязателен: на контуре модуль лежит не в plugins/, а в CI06932748/tools/,
# и пакета plugins там нет. Без него ломался не только этот файл — tfs_utils, который
# сам импортируется с фолбэком, падал на импорте s3_utils и уводил в Broken DAG весь тракт.
try:
    from plugins.utils import readable_size, get_conns_by_type  # type: ignore
except ImportError:
    from CI06932748.tools.utils import readable_size, get_conns_by_type  # type: ignore

from fnmatch import fnmatch
from datetime import date, datetime, timedelta, timezone
import base64
import re
import stat
import hashlib
import time
import io
import os
import gzip

from logging import getLogger
logger = getLogger("airflow.task")



def _md5_instead_of_checksum(request, **kwargs):
    """Возвращает Content-MD5 вместо контрольной суммы botocore.

    PutBucketLifecycleConfiguration — операция с requestChecksumRequired, и до
    botocore 1.36 SDK считал для неё Content-MD5. Потом его заменили на «гибкие
    контрольные суммы»: уходят x-amz-checksum-crc32 и x-amz-sdk-checksum-algorithm,
    а Content-MD5 не уходит вовсе. Наш шлюз знает только старый заголовок и отвечает
    "Missing required header for this request: Content-MD5" — правило не ставится.

    Считаем MD5 сами и убираем новые заголовки, чтобы запрос выглядел ровно так,
    как выглядел до замены. Хук стоит на before-sign, поэтому заголовок попадает
    в подпись SigV4, а не приезжает неподписанным довеском.
    """
    for name in [h for h in request.headers if h.lower().startswith(('x-amz-checksum-', 'x-amz-sdk-checksum-'))]:
        del request.headers[name]

    body = request.body or b''
    if isinstance(body, str):
        body = body.encode()
    request.headers['Content-MD5'] = base64.b64encode(hashlib.md5(body, usedforsecurity=False).digest()).decode()


def s3_set_ttl(conn, bucket, days, prefix='', status='Enabled'):
    """Создаёт или обновляет lifecycle-правило '{prefix}DeleteAfter' на удаление объектов через days дней."""

    hook = S3Hook(aws_conn_id=conn)
    client = hook.get_conn()
    client.meta.events.register('before-sign.s3.PutBucketLifecycleConfiguration', _md5_instead_of_checksum)
    
    # 1. Пытаемся получить текущие правила
    try:
        current_cfg = client.get_bucket_lifecycle_configuration(Bucket=bucket)
        rules = current_cfg.get('Rules', [])
    except ClientError as e:
        # Если правил еще нет, начинаем с пустого списка
        if e.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
            rules = []
        else:
            raise e

    # 2. Создаем новое правило
    new_rule_id = f'{prefix}DeleteAfter'
    new_rule = {
        'ID': new_rule_id,
        'Status': status,
        'Filter': {'Prefix': prefix},
        'Expiration': {'Days': days},
    }

    # 3. Обновляем существующее правило с таким же ID или добавляем новое
    # (Чтобы не плодить дубликаты при повторном запуске)
    rule_exists = False
    for i, rule in enumerate(rules):
        if rule.get('ID') == new_rule_id:
            rules[i] = new_rule
            rule_exists = True
            break
    
    if not rule_exists:
        rules.append(new_rule)

    # 4. Отправляем обновленный список правил
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={'Rules': rules}
    )
    return response
    
def s3_get_ttl(conn, bucket):
    """Возвращает список lifecycle-правил бакета. При отсутствии правил — []."""

    hook = S3Hook(aws_conn_id=conn)
    client = hook.get_conn()
    
    try:
        response = client.get_bucket_lifecycle_configuration(Bucket=bucket)
        return response.get('Rules', [])
    except ClientError as e:
        # Если правил жизненного цикла нет, AWS вернет ошибку NoSuchLifecycleConfiguration
        if e.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
            return []
        raise e

def s3_del_ttl(conn, bucket):
    """Удаляет всю lifecycle-конфигурацию бакета. Игнорирует ошибку NoSuchLifecycleConfiguration."""

    hook = S3Hook(aws_conn_id=conn)
    client = hook.get_conn()
    
    try:
        # Используем специальный метод для удаления всей конфигурации
        response = client.delete_bucket_lifecycle(Bucket=bucket)
        return response
    except ClientError as e:
        # Если правил и так нет, S3 может вернуть ошибку — игнорируем её
        if e.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
            return []
        raise e

def s3_drop_ttl(conn, bucket, prefix=''):
    """Снимает правило `{prefix}DeleteAfter`, остальные оставляет. True — правило было.

    Нужно, когда папке перестали задавать срок: `s3_set_ttl` правила только добавляет и
    обновляет, а забытое правило продолжает удалять объекты силами самого хранилища —
    асинхронно и без единой строки в отчёте. `s3_del_ttl` здесь не годится: он сносит всю
    конфигурацию бакета вместе с чужими правилами.
    """

    hook = S3Hook(aws_conn_id=conn)
    client = hook.get_conn()
    client.meta.events.register('before-sign.s3.PutBucketLifecycleConfiguration', _md5_instead_of_checksum)

    try:
        rules = client.get_bucket_lifecycle_configuration(Bucket=bucket).get('Rules', [])
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
            return False
        raise

    rule_id = f'{prefix}DeleteAfter'
    kept = [rule for rule in rules if rule.get('ID') != rule_id]
    if len(kept) == len(rules):
        return False

    if kept:
        client.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration={'Rules': kept})
    else:
        # Пустой список правил S3 не принимает — снимаем конфигурацию целиком
        client.delete_bucket_lifecycle(Bucket=bucket)
    return True


def s3_create_bucket(conn, bucket):
    """Создаёт бакет bucket в соединении conn, если он ещё не существует."""
    
    hook = S3Hook(aws_conn_id=conn)
    if not hook.check_for_bucket(bucket):
        hook.create_bucket(bucket_name=bucket)

def s3_bucket_size(conn, bucket, readable=False):
    """Обходит все объекты бакета (paginator) и возвращает {size, objs, min_date, max_date}.

    readable=True — размер и количество в человекочитаемом формате (KB/MB/K/M).
    """
    total_size = 0
    total_objs = 0
    min_date = None
    max_date = None
    
    _, paginator = s3_get_pages(conn, bucket)
    
    for page in paginator:
        if contents := page.get("Contents"):
            for obj in contents:
                last_mod = obj.get("LastModified")
                size = obj.get("Size", 0)
                
                total_size += size
                total_objs += 1
                
                # Обновляем мин/макс даты
                if last_mod:
                    if min_date is None or last_mod < min_date:
                        min_date = last_mod
                    if max_date is None or last_mod > max_date:
                        max_date = last_mod
    
    result = {
        "size": readable_size(total_size) if readable else total_size,
        "objs": readable_size(total_objs, base=1000) if readable else total_objs,
        "min_date": min_date.isoformat() if min_date else None,
        "max_date": max_date.isoformat() if max_date else None
    }
    return result

def s3_get_buckets(conn):
    """Возвращает список имён всех бакетов в соединении conn."""
    
    hook = S3Hook(aws_conn_id=conn, verify=False)
    s3_client = hook.get_conn() 
        
    response = s3_client.list_buckets()
    return [bucket['Name'] for bucket in response['Buckets']]

# Заполнение учётки к квоте: warn / error
S3_FILL_WARN, S3_FILL_ERROR = 0.8, 0.95


def s3_account_usage(conn, minutes, known=None, on_bucket=None) -> dict:
    """Квота учётки S3 и занятое всеми её бакетами: ``{'quota', 'buckets': {бакет: байт}, 'full'}``.

    Лимит выдаётся на учётку, а учётка видит только свои бакеты (``s3`` и ``s3-archive`` —
    разные учётки на одном сервере, бакеты у них не пересекаются), поэтому занятое — сумма по
    ``list_buckets``. Квота и объём бакета — из заголовков ``HEAD bucket`` Ceph RGW
    (``x-rgw-quota-user-size``, ``x-rgw-bytes-used``); в стандартном S3 их нет, MinIO отдаёт
    квоту только admin API — тогда её задаёт вызывающий (параметр ``quota_gb``).

    ``known`` — ``{бакет: (байт, полный ли)}`` для уже измеренных вызывающим бакетов (бакет логов
    меряет обход ``tools_log_cleanup``). Прочие бакеты без заголовка листаются ради суммы, не
    дольше ``minutes``; не уложились или ``minutes=0`` (квота неизвестна, считать незачем) —
    ``full=False``, занятое — нижняя оценка. Заголовки пишутся в лог: по ним видно, что отдаёт
    корпоративный шлюз.
    """
    # ponytail: имена заголовков RGW по документации, на корпоративном шлюзе не проверены
    client = S3Hook(aws_conn_id=conn, verify=False).get_conn()
    known = known or {}
    deadline = time.monotonic() + minutes * 60
    quota, buckets, full = None, {}, True

    def num(headers, name):
        try:
            value = int(headers.get(name, -1))
        except ValueError:
            return None
        return value if value >= 0 else None

    try:
        names = [item['Name'] for item in client.list_buckets()['Buckets']]
    except Exception as e:
        logger.warning(f"⚠️ список бакетов учётки {conn} не получен: {e}")
        names, full = list(known), False
    for name in names:
        started = time.monotonic()
        try:
            headers = client.head_bucket(Bucket=name)['ResponseMetadata']['HTTPHeaders']
        except Exception as e:
            logger.warning(f"⚠️ HEAD {name} не прочитан: {e}")
            headers = {}
        logger.info(f"HEAD {name}: {headers}")
        quota = quota or num(headers, 'x-rgw-quota-user-size') or None
        size = num(headers, 'x-rgw-bytes-used')
        if size is None and name in known:
            size, done = known[name]
            full = full and done
        elif size is None and not minutes:
            full = False
            continue
        elif size is None:
            size = 0
            for page in client.get_paginator('list_objects_v2').paginate(Bucket=name):
                size += sum(obj.get('Size', 0) for obj in page.get('Contents') or [])
                if time.monotonic() > deadline:
                    full = False
                    break
        buckets[name] = size
        if on_bucket:
            on_bucket(name, size, time.monotonic() - started)
    return {'quota': quota, 'buckets': buckets, 'full': full}


def s3_fill_check(account, quota_gb=0):
    """Вердикт здоровья ``fill`` по итогу ``s3_account_usage``: занятое к квоте учётки.

    Квота — из заголовков хранилища, иначе ``quota_gb``; нет обеих — None (проверки нет).
    ⚠️ от 80 %, ❌ от 95 %; в сводке три крупнейших бакета — видно, кто съедает лимит.
    """
    quota = account.get('quota') or (quota_gb or 0) * 1024 ** 3
    if not quota:
        return None
    buckets = account.get('buckets') or {}
    used = sum(buckets.values())
    share = used / quota
    status = 'error' if share >= S3_FILL_ERROR else 'warn' if share >= S3_FILL_WARN else 'healthy'
    top = ', '.join(f"`{name}` {readable_size(size)}"
                    for name, size in sorted(buckets.items(), key=lambda item: -item[1])[:3])
    return {'status': status, 'summary': (
        f"учётка S3: занято {readable_size(used)} из {readable_size(quota)} ({share:.0%}"
        + (', квота из quota_gb' if not account.get('quota') else '') + f"); крупнейшие: {top}"
        + ('' if account.get('full', True) else '; подсчёт неполный — занято не меньше'))}


def s3_get_pages(conn, bucket, prefix='', page_size=1000, max_items=10000):
    """Создаёт S3-paginator для list_objects_v2 и возвращает (hook, pages_iterator)."""
    
    hook = S3Hook(aws_conn_id=conn)
    s3_client = hook.get_conn()
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(
        Bucket=bucket, 
        Prefix=prefix,
        PaginationConfig={'MaxItems': max_items, 'PageSize': page_size}
    )
    return hook, pages

def s3_path_parse(path: str) -> dict:
    """
    Разбирает S3 путь на составляющие и определяет наличие маски.

    Схему режем вручную, а не urlparse: в нашем формате слева от '://' стоит conn_id,
    а он не обязан быть валидной URL-схемой. RFC 3986 разрешает в схеме только буквы,
    цифры, '+', '-' и '.', поэтому на 's3_minio://bucket/key' urlparse молча отдаёт
    пустые scheme и netloc — и дальше по коду улетает пустое имя бакета.
    """
    head, sep, rest = path.partition('://')
    if sep:
        conn_id = head
        bucket, _, full_path = rest.partition('/')
    else:
        conn_id, bucket, full_path = '', '', path.lstrip('/')
    
    # Поиск спецсимволов
    star_pos = full_path.find('*')
    qmark_pos = full_path.find('?')
    is_mask = star_pos != -1 or qmark_pos != -1
    
    # Находим позицию самого первого спецсимвола
    special_chars = [pos for pos in [star_pos, qmark_pos] if pos != -1]
    wild_pos = min(special_chars) if special_chars else len(full_path)
    
    if is_mask:
        # Ищем последний слеш ДО первого спецсимвола
        last_slash = full_path.rfind('/', 0, wild_pos)
        if last_slash == -1:
            prefix = ""
            mask = full_path
        else:
            prefix = full_path[:last_slash]
            mask = full_path[last_slash+1:]
    else:
        # Если маски нет, стандартное деление на папку и файл
        prefix = os.path.dirname(full_path)
        mask = os.path.basename(full_path)

    name, ext = os.path.splitext(mask)
    
    return {
        "path":    path,
        "conn_id": conn_id,
        "bucket":  bucket,
        "key":     full_path,
        "prefix":  (prefix + '/') if prefix else '',
        "file":    mask,
        "name":    name,
        "ext":     ext,
        "is_mask": is_mask,
    }
    
class s3_IterStream(io.RawIOBase):
    """Оборачивает генератор байтов в file-like объект (RawIOBase) для boto3 upload_fileobj.

    Буферизует чанки из итератора по мере чтения. Защита от OOM:
    предупреждение при буфере > 100 МБ, MemoryError при > 512 МБ.
    total_read — суммарное количество прочитанных байт.
    """
    def __init__(self, iterator):
        self.iterator = iterator
        self.buffer = b''
        self.total_read = 0  

    def readable(self): 
        return True

    def read(self, size=-1):
        # Пороги памяти
        WARN_THRESHOLD = 100 * 1024 * 1024  # 100 MB - предупреждение в логи
        MAX_THRESHOLD = 512 * 1024 * 1024   # 512 MB - жесткая остановка (защита воркера)

        if size == -1:
            # Набираем по чанку и смотрим на порог по дороге. Было
            # b''.join(self.iterator): весь поток складывался в память, и MemoryError
            # поднимался уже после того, как память занята — защита срабатывала, когда
            # защищать было нечего. Порционная ветка ниже всегда считала правильно.
            buf = bytearray(self.buffer)
            self.buffer = b''
            for chunk in self.iterator:
                if not chunk:
                    continue
                buf.extend(chunk)
                if len(buf) > MAX_THRESHOLD:
                    raise MemoryError(f"🚨 Buffer exceeded MAX_THRESHOLD: {readable_size(len(buf))}")
            if len(buf) > WARN_THRESHOLD:
                logger.warning(f"⚠️ IterStream buffer is large: {readable_size(len(buf))}")
            self.total_read += len(buf)
            return bytes(buf)

        while len(self.buffer) < size:
            try:
                chunk = next(self.iterator)
                if not chunk: 
                    break
                self.buffer += chunk
                
                curr_len = len(self.buffer)
                if curr_len > MAX_THRESHOLD:
                    raise MemoryError(f"🚨 Critical Memory Limit! Buffer: {readable_size(curr_len)}")
                elif curr_len > WARN_THRESHOLD:
                    logger.warning(f"⚠️ IterStream buffer is large: {readable_size(curr_len)}")
                # else:
                #     logger.debug(f"📦 IterStream buffer: {readable_size(curr_len)}")
            except StopIteration:
                break

        res = self.buffer[:size]
        self.buffer = self.buffer[size:]
        self.total_read += len(res)
        return res

def s3_gzip_stream(iterator, compresslevel=6):
    """Генератор: потоково сжимает чанки из iterator в GZIP без буферизации всего файла в памяти."""
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode='wb', compresslevel=compresslevel) as gz:
        for chunk in iterator:
            gz.write(chunk)
            res = buffer.getvalue()
            if res:
                yield res
                buffer.seek(0)
                buffer.truncate()
    yield buffer.getvalue()

def s3_to_s3(src_path, dst_path, compress=False):
    """Копирует объект S3→S3. Одно соединение без сжатия — нативная copy_object.

    В остальных случаях — потоковое копирование через воркер с опциональным GZIP.
    Сжатие отключается автоматически для .parquet/.zip/.gz.
    """
    src = s3_path_parse(src_path)
    dst = s3_path_parse(dst_path)

    if src['ext'].lower() in  ['.parquet', '.zip', '.gz']:
        if compress:
            logger.info(f"⏩ Compression disabled for {src['ext']} (already compressed)")
        compress = False
    
    s_hook = S3Hook(aws_conn_id=src['conn_id'])
    
    final_key = dst['key']
    
    # ОПТИМИЗАЦИЯ: Если одно подключение и сжимать не надо
    if src['conn_id'] == dst['conn_id'] and not compress:
        logger.info(f"🚀 S3 Native Copy {src_path} to {final_key}")
        s_hook.copy_object(
            source_bucket_key=src['key'],
            dest_bucket_key=dst['key'],
            source_bucket_name=src['bucket'],
            dest_bucket_name=dst['bucket']
        )
        return
    # В ОСТАЛЬНЫХ СЛУЧАЯХ: Потоковое копирование через воркер
    d_client = S3Hook(aws_conn_id=dst['conn_id']).get_conn()
    
    # Поток из источника
    body_stream = s_hook.get_key(src['key'], src['bucket']).get()['Body']
    
    data_iter = body_stream # По умолчанию просто поток байтов
    
    if compress:
        data_iter = s3_gzip_stream(body_stream)
        if not final_key.endswith('.gz'):
            final_key += '.gz'
            dst_path += '.gz'

    config = TransferConfig(use_threads=False)
    d_client.upload_fileobj(s3_IterStream(data_iter), dst['bucket'], final_key, Config=config)
    logger.info(f"✅ Copied {src_path} to {dst_path}")

def s3_delete(path):
    """Удаляет объект из S3 по пути вида conn_id://bucket/key."""
    s3 = s3_path_parse(path)
    hook = S3Hook(aws_conn_id=s3['conn_id'])
    hook.delete_objects(keys=s3['key'],  bucket=s3['bucket'])
    logger.info(f"🗑️ Deleted: {path}")

def s3_move_s3(src_path, dst_path, compress=False, done=False):
    """Перемещает объект S3→S3: копирует через s3_to_s3, затем удаляет источник.

    done=True или done=<путь> — дополнительно создаёт .done-маркер.
    """
    s3_to_s3(src_path, dst_path, compress=compress)
    if done:
        s3_done(done if isinstance(done, str) else dst_path )
    s3_delete(src_path)
    logger.info(f"✅ Moved: {src_path} to {dst_path}")

def s3_done(path):
    """Создаёт пустой файл-маркер {path}.done в S3."""
    s3 = s3_path_parse(path)
    hook = S3Hook(aws_conn_id=s3['conn_id'])
    hook.load_string(
            bucket_name=s3['bucket'],
            key=s3['key'] + '.done',
            string_data='',
            replace=True
        )
    logger.info(f"✅ Done-файл создан: {path}.done")
    
def s3_from_zip(src_path, dst_prefix, compress=True):
    """Потоково извлекает файлы из ZIP в S3 и загружает их в dst_prefix (без сохранения на диск).

    compress=True — каждый файл дополнительно сжимается в GZIP (кроме .parquet/.zip/.gz).
    Имена файлов декодируются из cp866. Возвращает (total_bytes, total_files, stats_dict).
    """
    start_time = time.time()
    src = s3_path_parse(src_path)
    dst = s3_path_parse(dst_prefix)
    
    s_hook = S3Hook(aws_conn_id=src['conn_id'])
    d_client = S3Hook(aws_conn_id=dst['conn_id']).get_conn()
    
    body = s_hook.get_key(src['key'], src['bucket']).get()['Body']
    config = TransferConfig(use_threads=False)

    total_uploaded_size = 0
    total_raw_size = 0
    total_count = 0

    for f_name_byte, f_size, chunks in stream_unzip(body):
        f_name = f_name_byte.decode('cp866', errors='replace').strip('/')
        if not f_name or f_name.endswith('/'): 
            continue
        
        target_key = f"{dst['key']}/{f_name}"
        
        # Если f_size известен заранее, плюсуем его
        if f_size: total_raw_size += f_size
        
        # Проверяем расширение текущего файла f_name
        not_zip = True
        if any(f_name.lower().endswith(ext) for ext in ['.parquet', '.zip', '.gz']):
            not_zip = False
            if compress:
                logger.info(f"⏩ Compression disabled for {f_name} (already compressed)")
        
        if compress and not_zip:
            final_gen = s3_gzip_stream(chunks)
            if not target_key.endswith('.gz'):
                target_key += ".gz"
        else:
            final_gen = chunks

        stream = s3_IterStream(final_gen)
        d_client.upload_fileobj(stream, dst['bucket'], target_key, Config=config)

        total_uploaded_size += stream.total_read
        total_count += 1
        
        # Если f_size был None (потоковый ZIP), считаем по факту прочитанного из IterStream (без сжатия)
        if not f_size and not compress:
            total_raw_size += stream.total_read

    # Расчет статистики
    duration = time.time() - start_time
    # Скорость в сек (по исходным данным)
    speed = total_raw_size / duration if duration > 0 else 0
    
    stats = {
        "count": readable_size(total_count, 1000),
        "size": readable_size(total_uploaded_size),
        "raw_size": readable_size(total_raw_size),
        "duration_sec": round(duration, 2),
        "speed_in_s": readable_size(speed)
    }

    logger.info(
        f"🏁 Done: {stats['count']} files | "
        f"Uploaded: {stats['size']} | "
        f"Speed: {stats['speed_in_s']} in sec"
    )

    return total_uploaded_size, total_count, stats

MSK = timezone(timedelta(hours=3))  # MSK круглый год UTC+3


def s3_to_zip(src_path, dst_path, date_from=None, date_to=None, max_bytes=0, replace=False, part_size=64 * 1024**2,
              name=None, ts=None):
    """Потоково собирает объекты S3 в один ZIP64 в S3 — без диска и без буфера под весь архив.

    src_path — conn_id://bucket/путь: без маски берёт всё под путём, с маской (* ?) — по fnmatch.
    dst_path на .zip — ключ архива, иначе папка: имя name (по умолчанию последняя папка src).
    ts — метка: к имени добавляется _<ts>, в том числе перед .zip ключа. Пустой отбор — skip.
    date_from/date_to — 'YYYY-MM-DD' по LastModified в MSK (включительно). max_bytes — лимит
    сырого объёма (0 — без лимита). Своя multipart-загрузка: при сбое отменяется, хвостов нет;
    часть растёт с объёмом, чтобы не упереться в 10 000 частей. В памяти — буфер части и
    список отобранных ключей кортежами (~200 байт на объект): лимит нужен до записи.
    Хвосты жёстко убитого пода (OOM, SIGKILL — отмена не успела) снимаются в начале следующего
    вызова: незавершённые загрузки этого же имени (<имя>.zip, <имя>_<YYYYMMDD_HHMMSS>.zip) в папке
    архива — чужие имена (другие src того же рана) не трогаются.
    Уже сжатые файлы — deflate уровня 0: NO_COMPRESSION_64 держит файл в памяти ради CRC.
    """
    src, dst = s3_path_parse(src_path), s3_path_parse(dst_path)
    base = src['prefix'] if src['is_mask'] else (src['key'].rstrip('/') + '/' if src['key'] else '')
    try:
        d_from, d_to = (date.fromisoformat(d) if d else None for d in (date_from, date_to))
    except ValueError as e:
        raise AirflowFailException(f"Формат дат — YYYY-MM-DD: {e}")

    dst_key = dst['key']
    if dst_key.lower().endswith('.zip'):
        folder, _, stem = dst_key.rpartition('/')
        folder, stem = folder + '/' if folder else '', stem[:-4]
    else:
        folder = dst_key.rstrip('/') + '/' if dst_key else ''
        stem = name or base.rstrip('/').rsplit('/', 1)[-1] or src['bucket']
    dst_key = f"{folder}{stem}{'_' + ts if ts else ''}.zip"
    out = f"{dst['conn_id']}://{dst['bucket']}/{dst_key}"
    s_cli = S3Hook(aws_conn_id=src['conn_id']).get_conn()
    d_cli = S3Hook(aws_conn_id=dst['conn_id']).get_conn()
    if not replace:
        try:
            d_cli.head_object(Bucket=dst['bucket'], Key=dst_key)
            raise AirflowFailException(f"Архив уже есть: {out} (replace=False)")
        except ClientError as e:
            if e.response['Error']['Code'] not in ('404', 'NoSuchKey', 'NotFound'):
                raise

    own = re.compile(re.escape(stem) + r'(_\d{8}_\d{6})?\.zip', re.IGNORECASE)
    aborted = []
    for page in d_cli.get_paginator('list_multipart_uploads').paginate(Bucket=dst['bucket'], Prefix=folder):
        for u in page.get('Uploads', []):
            if own.fullmatch(u['Key'][len(folder):]):
                d_cli.abort_multipart_upload(Bucket=dst['bucket'], Key=u['Key'], UploadId=u['UploadId'])
                aborted.append(u['Key'])
    if aborted:
        logger.warning(f"🧹 Отменены брошенные загрузки: {aborted}")

    objs = []
    for page in s_cli.get_paginator('list_objects_v2').paginate(Bucket=src['bucket'], Prefix=base):
        for o in page.get('Contents', []):
            k, day = o['Key'], o['LastModified'].astimezone(MSK).date()
            if k.endswith('/') or (src['is_mask'] and not fnmatch(k, src['key'])) \
                    or (src['conn_id'] == dst['conn_id'] and src['bucket'] == dst['bucket'] and k == dst_key) \
                    or (d_from and day < d_from) or (d_to and day > d_to):
                continue
            objs.append((k, o['Size'], o['LastModified']))
    raw = sum(o[1] for o in objs)
    if not objs:
        raise AirflowSkipException(f"Под {src_path} нечего архивировать (даты MSK {date_from}…{date_to})")
    if max_bytes and raw > max_bytes:
        raise AirflowFailException(
            f"{len(objs)} объектов, {readable_size(raw)} — больше лимита {readable_size(max_bytes)}")
    logger.info(f"📦 {len(objs)} объектов, {readable_size(raw)} → {out}")

    def members():
        for k, sz, mtime in objs:
            level = 0 if k.lower().endswith(('.gz', '.zip', '.parquet')) else 6
            body = s_cli.get_object(Bucket=src['bucket'], Key=k)['Body']
            yield k[len(base):], mtime, stat.S_IFREG | 0o644, ZIP_AUTO(sz, level=level), body.iter_chunks(1024**2)

    # Архив не больше сырого объёма с заголовками; 9 000 частей — запас до лимита S3 в 10 000
    part_size = max(part_size, (raw + 1024 * len(objs)) // 9000 + 1)

    start, size, parts, buf = time.time(), 0, [], bytearray()
    upload_id = d_cli.create_multipart_upload(Bucket=dst['bucket'], Key=dst_key)['UploadId']

    def flush():
        n = len(parts) + 1
        etag = d_cli.upload_part(Bucket=dst['bucket'], Key=dst_key, UploadId=upload_id, PartNumber=n, Body=bytes(buf))['ETag']
        parts.append({'PartNumber': n, 'ETag': etag})
        buf.clear()

    try:
        for chunk in stream_zip(members()):
            buf += chunk
            size += len(chunk)
            if len(buf) >= part_size:
                flush()
        if buf or not parts:
            flush()
        d_cli.complete_multipart_upload(Bucket=dst['bucket'], Key=dst_key, UploadId=upload_id,
                                        MultipartUpload={'Parts': parts})
    except BaseException:
        try:
            d_cli.abort_multipart_upload(Bucket=dst['bucket'], Key=dst_key, UploadId=upload_id)
        except Exception as e:  # исходная ошибка важнее: хвост снимет следующий запуск (REQ-s3zip-11)
            logger.error(f"❌ Не отменена загрузка {out}: {e}")
        raise
    sec = round(time.time() - start, 1)
    stats = {'path': out, 'count': len(objs), 'raw': raw, 'size': size, 'sec': sec,
             'speed': readable_size(raw / sec if sec else raw) + '/с', 'aborted': aborted}
    logger.info(f"🏁 {out}: {len(objs)} файлов, {readable_size(raw)} → {readable_size(size)} за {sec} с")
    return stats


def s3_keys(path) -> dict:
    """Возвращает dict {key: 'дата / размер'} объектов S3, соответствующих маске в path.

    path вида conn_id://bucket/prefix/mask, поддерживает * и ?. Пропускает директории и .error.
    """
    
    s3 = s3_path_parse(path)
    hook = S3Hook(aws_conn_id=s3['conn_id'])

    # Получаем список объектов с их метаданными
    # list_objects_v2 возвращает список словарей, где есть 'Key' и 'LastModified'
    s3_client = hook.get_conn()
    paginator = s3_client.get_paginator('list_objects_v2')
    
    objects = {}
    
    for page in paginator.paginate(
            Bucket=s3['bucket'], 
            Prefix=s3['prefix'],
            PaginationConfig={'PageSize': 1000, 'MaxItems': 10_000, }
        ):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith('/') or key.endswith('.error'): 
                    continue
                logger.debug(f"🔍 {key} {obj}")
                
                # Фильтруем по wildcard (аналог wildcard_match)
                if fnmatch(key, s3['key']):
                    # LastModified boto3 отдаёт уже как aware datetime — оборачивать не надо
                    dt = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S %Z')
                    objects[key] = dt + f" / {readable_size(obj['Size'])}"
                    logger.info(f"✅ {key}: {objects[key]}")
    
    return objects

_s3_list = None

def get_s3_list():
    """Ленивый синглтон: список URI всех S3-бакетов по всем aws-соединениям вида 'conn_id://bucket/'.

    Выполняет сетевые запросы только при первом вызове, далее возвращает кеш.
    """
    global _s3_list
    if _s3_list is None:
        _s3_list = []
        for conn in get_conns_by_type(conn_type='aws'):
            try:
                for bucket in s3_get_buckets(conn):
                    _s3_list.append(f"{conn}://{bucket}/")
            except Exception:
                _s3_list.append(f"{conn}://")
    return _s3_list

