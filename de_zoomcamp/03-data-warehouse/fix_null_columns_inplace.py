import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import RLock
from typing import Optional, Tuple, List

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from tqdm import tqdm

tqdm.set_lock(RLock())

BUCKET_NAME = "vs-data-engineering-zoomcamp-bucket"

# Какие колонки нормализуем в каких префиксах
FIX_MAP = {
    "yellow/": "airport_fee",
    "green/": "ehail_fee",
}


def list_parquet_blobs(client: storage.Client, prefix: str) -> List[storage.Blob]:
    bucket = client.bucket(BUCKET_NAME)
    return [b for b in client.list_blobs(bucket, prefix=prefix) if b.name.endswith(".parquet")]


def get_arrow_type_from_gcs_blob(blob: storage.Blob) -> Optional[pa.DataType]:
    """
    Читает только метаданные parquet (быстро), возвращает Arrow тип колонки,
    либо None если колонки нет.
    """
    # blob.open("rb") умеет range-reads, и pyarrow обычно читает footer без скачивания всего файла
    with blob.open("rb") as f:
        pf = pq.ParquetFile(f)
        schema = pf.schema_arrow
    field = schema.field(schema.get_field_index(blob._target_column)) if hasattr(blob, "_target_column") else None
    # (мы ниже установим _target_column, чтобы не протаскивать отдельно)
    return None if field is None else field.type


def needs_fix(blob: storage.Blob, target_col: str) -> bool:
    """
    Проверка по метаданным: нужно ли исправлять файл.
    - если колонка отсутствует -> будем добавлять (тоже "fix")
    - если тип null -> fix
    - если тип не float64 -> fix (на всякий)
    """
    blob._target_column = target_col  # хак, чтобы не менять сигнатуру helper'а
    with blob.open("rb") as f:
        pf = pq.ParquetFile(f)
        schema = pf.schema_arrow

    if target_col not in schema.names:
        return True

    t = schema.field(target_col).type
    if pa.types.is_null(t):
        return True
    if not pa.types.is_floating(t):
        return True
    # если float, но не float64 — тоже приведём к float64
    if str(t) != "double":
        return True

    return False


def _stream_blob_to_file(blob: storage.Blob, out_path: str, position: int, desc: str, chunk_size: int = 8 * 1024 * 1024):
    blob.reload()  # чтобы заполнить blob.size
    total = blob.size

    with blob.open("rb") as r, open(out_path, "wb") as w, tqdm(
        total=total,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        position=position,
        leave=False,
        desc=f"DL {desc}",
    ) as pbar:
        while True:
            chunk = r.read(chunk_size)
            if not chunk:
                break
            w.write(chunk)
            pbar.update(len(chunk))


def _stream_file_to_blob(in_path: str, blob: storage.Blob, position: int, desc: str, chunk_size: int = 8 * 1024 * 1024):
    total = os.path.getsize(in_path)

    # Перезапишем объект целиком
    with open(in_path, "rb") as r, blob.open("wb") as w, tqdm(
        total=total,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        position=position,
        leave=False,
        desc=f"UL {desc}",
    ) as pbar:
        while True:
            chunk = r.read(chunk_size)
            if not chunk:
                break
            w.write(chunk)
            pbar.update(len(chunk))


def fix_one_blob(client: storage.Client, blob_name: str, position: int) -> Tuple[str, str]:
    """
    Возвращает (blob_name, status): status in {"skipped", "fixed"}.
    """
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)

    # определяем, какую колонку правим
    target_col = None
    for pref, col in FIX_MAP.items():
        if blob_name.startswith(pref):
            target_col = col
            break
    if target_col is None:
        return blob_name, "skipped"

    # проверка — нужно ли вообще трогать
    if not needs_fix(blob, target_col):
        return blob_name, "skipped"

    # Временные файлы
    fd_in, path_in = tempfile.mkstemp(suffix=".parquet")
    fd_out, path_out = tempfile.mkstemp(suffix=".parquet")
    os.close(fd_in)
    os.close(fd_out)

    try:
        # 1) Скачать из GCS с прогрессом
        _stream_blob_to_file(blob, path_in, position=position, desc=blob_name)

        # 2) Прочитать и переписать parquet с приведением типа
        table = pq.read_table(path_in)

        if target_col not in table.column_names:
            # добавим float64 nulls
            arr = pa.nulls(table.num_rows, type=pa.float64())
            table = table.append_column(target_col, arr)
        else:
            col = table[target_col]
            # если это NullType или что-то не float64 — приводим
            col_fixed = col.cast(pa.float64())
            idx = table.schema.get_field_index(target_col)
            table = table.set_column(idx, target_col, col_fixed)

        # пишем заново (snappy как обычно у NYC taxi parquet)
        pq.write_table(table, path_out, compression="snappy")

        # 3) Залить обратно (in-place overwrite) с прогрессом
        _stream_file_to_blob(path_out, blob, position=position, desc=blob_name)

        return blob_name, "fixed"

    finally:
        for p in (path_in, path_out):
            try:
                os.remove(p)
            except OSError:
                pass


def build_jobs(client: storage.Client) -> List[str]:
    jobs = []
    for prefix in FIX_MAP.keys():
        blobs = list_parquet_blobs(client, prefix)
        jobs.extend([b.name for b in blobs])
    return jobs


def main(max_workers: int = 4):
    client = storage.Client()
    jobs = build_jobs(client)

    fixed = 0
    skipped = 0

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = []
        for i, name in enumerate(jobs):
            futures.append(ex.submit(fix_one_blob, client, name, i))

        for fut in as_completed(futures):
            name, status = fut.result()
            if status == "fixed":
                fixed += 1
            else:
                skipped += 1

    print(f"Done. fixed={fixed}, skipped={skipped}, total={len(jobs)}")


if __name__ == "__main__":
    main(max_workers=4)