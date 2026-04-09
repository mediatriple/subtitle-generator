import whisper
from whisper.utils import get_writer
import subprocess
import pika
import json
import os
import socket
import importlib
import contextlib
import time
import requests
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

queue_name = 'subtitle_gen_queue'
model_type = os.getenv("WHISPER_MODEL_TYPE", "large")
subtitle_type = 'vtt'
models_dir = './models/'
hostname = os.getenv("RABBITMQ_HOST")
rabbitmq_port = int(os.getenv("RABBITMQ_PORT") or 5672)
username = os.getenv("RABBITMQ_USERNAME") or os.getenv("RABBITMQ_USER")
password = os.getenv("RABBITMQ_PASSWORD") or os.getenv("RABBITMQ_PASS")
storage_path = os.getenv("STORAGE_PATH", "/data")
panel_version_default = int(os.getenv("PANEL_VERSION", "1"))
panel_v1_base_url = os.getenv("PANEL_V1_BASE_URL", "https://panel.videoon.ly").rstrip("/")
panel_v2_base_url = os.getenv("PANEL_V2_BASE_URL", "https://panelpp.mediatriple.net").rstrip("/")
panel_v2_status_path = os.getenv("PANEL_V2_STATUS_PATH", "/api/encoder/update-subtitle-status").strip()
panel_v2_api_key = os.getenv("PANEL_V2_API_KEY") or os.getenv("API_KEY", "")
rabbitmq_ack_mode = (os.getenv("RABBITMQ_ACK_MODE") or "late").strip().lower()
duplicate_message_policy = (os.getenv("RABBITMQ_DUPLICATE_POLICY") or "requeue").strip().lower()
processing_lock_ttl_seconds = int(os.getenv("PROCESSING_LOCK_TTL_SECONDS") or 21600)
if panel_v2_status_path and not panel_v2_status_path.startswith("/"):
    panel_v2_status_path = f"/{panel_v2_status_path}"
panel_base_urls = {
    1: panel_v1_base_url,
    2: panel_v2_base_url,
}
whisper_model = None

if rabbitmq_ack_mode not in {"late", "early"}:
    print(
        f"Invalid RABBITMQ_ACK_MODE '{rabbitmq_ack_mode}'. "
        "Supported values: 'late', 'early'. Falling back to 'late'."
    )
    rabbitmq_ack_mode = "late"

if duplicate_message_policy not in {"requeue", "ack"}:
    print(
        f"Invalid RABBITMQ_DUPLICATE_POLICY '{duplicate_message_policy}'. "
        "Supported values: 'requeue', 'ack'. Falling back to 'requeue'."
    )
    duplicate_message_policy = "requeue"

if processing_lock_ttl_seconds <= 0:
    print(
        f"Invalid PROCESSING_LOCK_TTL_SECONDS '{processing_lock_ttl_seconds}'. "
        "It must be greater than 0. Falling back to 21600."
    )
    processing_lock_ttl_seconds = 21600


def get_rabbitmq_hosts(primary_host):
    return [primary_host]


def format_error(err):
    message = str(err).strip()
    return message if message else repr(err)


def normalize_path(path):
    raw_path = str(path or "").strip()
    if not raw_path:
        return raw_path
    return os.path.normpath(raw_path)


def subtitle_file_exists_and_non_empty(path):
    try:
        return bool(path) and os.path.exists(path) and os.path.getsize(path) > 0
    except OSError:
        return False


def build_processing_lock_path(cc_path):
    return f"{cc_path}.lock"


def acquire_processing_lock(lock_path, cs_id):
    lock_dir = os.path.dirname(lock_path)
    if lock_dir:
        os.makedirs(lock_dir, exist_ok=True)

    lock_payload = json.dumps(
        {
            "cs_id": int(cs_id),
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "created_at_unix": int(time.time()),
        }
    )

    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w", encoding="utf-8") as lock_file:
            lock_file.write(lock_payload)
        return True, "acquired"
    except FileExistsError:
        pass
    except Exception as lock_create_error:
        return True, f"lock_bypassed_create_error={format_error(lock_create_error)}"

    try:
        lock_age_seconds = int(time.time() - os.path.getmtime(lock_path))
    except Exception as lock_age_error:
        return True, f"lock_bypassed_age_check_error={format_error(lock_age_error)}"

    if lock_age_seconds > processing_lock_ttl_seconds:
        try:
            os.remove(lock_path)
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as lock_file:
                lock_file.write(lock_payload)
            return True, f"replaced_stale_lock_age={lock_age_seconds}s"
        except Exception as stale_lock_error:
            return False, f"stale_lock_replace_failed={format_error(stale_lock_error)}"

    return False, f"active_lock_age={lock_age_seconds}s"


def release_processing_lock(lock_path):
    try:
        if lock_path and os.path.exists(lock_path):
            os.remove(lock_path)
    except Exception as lock_release_error:
        print(f"Warning: Could not remove processing lock '{lock_path}': {format_error(lock_release_error)}")


def create_rabbitmq_connection():
    if not hostname or not rabbitmq_port or not username or not password:
        raise RuntimeError(
            "RABBITMQ_HOST, RABBITMQ_PORT ve kullanıcı/şifre environment değişkenleri tanımlı olmalı. "
            "Kullanıcı için: RABBITMQ_USERNAME veya RABBITMQ_USER. "
            "Şifre için: RABBITMQ_PASSWORD veya RABBITMQ_PASS."
        )

    last_error = None

    for host in get_rabbitmq_hosts(hostname):
        try:
            socket.getaddrinfo(host, rabbitmq_port)
        except socket.gaierror as dns_error:
            last_error = dns_error
            print(f"Skipping host {host}: DNS resolution failed ({format_error(dns_error)})")
            continue

        try:
            print(
                f"Trying RabbitMQ host: {host}:{rabbitmq_port} "
                f"with user '{username}'"
            )
            connection_params = pika.ConnectionParameters(
                host=host,
                port=rabbitmq_port,
                credentials=pika.PlainCredentials(username, password),
                heartbeat=0,
                connection_attempts=1,
                socket_timeout=5,
                blocked_connection_timeout=5,
            )
            return pika.BlockingConnection(connection_params)
        except Exception as conn_error:
            last_error = conn_error
            print(
                f"Failed to connect host {host}:{rabbitmq_port} "
                f"with user '{username}' ({format_error(conn_error)})"
            )

    raise RuntimeError(
        f"Could not connect to RabbitMQ. Last error: {format_error(last_error)}. "
        "Check RabbitMQ endpoint and credentials."
    )


def resolve_panel_version(panel_version):
    try:
        return int(panel_version)
    except (TypeError, ValueError):
        return panel_version_default


def map_status_for_panel_v2(status):
    normalized_status = str(status or "").strip().upper()
    status_map = {
        "IN_QUEUE": "queued",
        "QUEUED": "queued",
        "GENERATING": "generating",
        "GENERATED": "ready",
        "READY": "ready",
        "ERROR": "error",
    }
    return status_map.get(normalized_status, "error")


def update_cc_status_panel_v1(cs_id, status, base_url):
    status_url = f"{base_url}/encoders/update_cc_status/{cs_id}/{status}"
    print(f"Updating CC status via panel v1: {status_url}")
    return requests.get(status_url, verify=False, timeout=10)


def normalize_progress_percentage(progress_percentage):
    try:
        value = float(progress_percentage)
    except (TypeError, ValueError):
        return None

    if value < 0:
        return 0.0
    if value > 100:
        return 100.0
    return round(value, 2)


def update_cc_status_panel_v2(
    cs_id,
    status,
    base_url,
    subtitle_file_path=None,
    error_message=None,
    progress_percentage=None,
):
    status_url = f"{base_url}{panel_v2_status_path}"
    payload = {
        "subtitle_id": int(cs_id),
        "status": map_status_for_panel_v2(status),
    }
    normalized_progress = normalize_progress_percentage(progress_percentage)
    if normalized_progress is not None:
        payload["progress_percentage"] = normalized_progress

    if payload["status"] == "ready":
        if not subtitle_file_path or not os.path.exists(subtitle_file_path):
            raise FileNotFoundError(f"Subtitle file not found for panel v2 upload: {subtitle_file_path}")
        with open(subtitle_file_path, "r", encoding="utf-8") as subtitle_file:
            payload["subtitle_content"] = subtitle_file.read()
    elif payload["status"] == "error" and error_message:
        payload["error_message"] = str(error_message)

    headers = {"Accept": "application/json"}
    if panel_v2_api_key:
        headers["X-API-KEY"] = panel_v2_api_key
    else:
        print("Warning: PANEL_V2_API_KEY is not set; panel v2 request may be unauthorized.")

    print(f"Updating CC status via panel v2: {status_url} ({payload['status']})")
    response = requests.post(
        status_url,
        json=payload,
        headers=headers,
        verify=False,
        timeout=30,
    )
    print(f"Panel v2 response: {response.status_code}")
    if not response.ok:
        print(f"Panel v2 response body: {response.text[:500]}")
    return response


def update_cc_status(
    cs_id,
    status,
    panel_version=None,
    subtitle_file_path=None,
    error_message=None,
    progress_percentage=None,
):
    resolved_panel_version = resolve_panel_version(panel_version)
    base_url = panel_base_urls.get(
        resolved_panel_version,
        panel_base_urls.get(panel_version_default, panel_v1_base_url),
    )

    try:
        if resolved_panel_version == 2:
            return update_cc_status_panel_v2(
                cs_id=cs_id,
                status=status,
                base_url=base_url,
                subtitle_file_path=subtitle_file_path,
                error_message=error_message,
                progress_percentage=progress_percentage,
            )
        return update_cc_status_panel_v1(cs_id=cs_id, status=status, base_url=base_url)
    except Exception as panel_error:
        print(f"Status update failed: {format_error(panel_error)}")
        return None


def get_filename(path): return os.path.splitext(os.path.basename(path))[0]


def preload_model():
    global whisper_model
    if whisper_model is None:
        os.makedirs(models_dir, exist_ok=True)
        print(f"Loading Whisper model '{model_type}' to '{models_dir}'...")
        # Suppress Whisper's tqdm download progress in pod logs.
        with open(os.devnull, "w") as devnull, contextlib.redirect_stderr(devnull):
            whisper_model = whisper.load_model(model_type, download_root=models_dir)
        print(f"Whisper model '{model_type}' is ready.")
    return whisper_model


def transcribe_with_progress(model, video_path, on_progress=None):
    transcribe_module = importlib.import_module("whisper.transcribe")
    original_tqdm = transcribe_module.tqdm.tqdm

    class PercentTqdm:
        def __init__(self, *args, **kwargs):
            self.total = kwargs.get("total") or 0
            self.current = 0
            self.last_percent = -1

        def __enter__(self):
            print("Transcription progress: 0%")
            self.last_percent = 0
            if callable(on_progress):
                on_progress(0)
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def update(self, n=1):
            self.current += n
            if not self.total:
                return
            percent = int(min(100, (self.current * 100) / self.total))
            if percent > self.last_percent:
                self.last_percent = percent
                print(f"Transcription progress: {percent}%")
                if callable(on_progress):
                    on_progress(percent)

    transcribe_module.tqdm.tqdm = PercentTqdm
    try:
        result = model.transcribe(
            video_path,
            language="en",
            task="translate",
            fp16=False,
            verbose=False,
        )
        print("Transcription progress: 100%")
        if callable(on_progress):
            on_progress(100)
        return result
    finally:
        transcribe_module.tqdm.tqdm = original_tqdm


def generate_cc(cs_id, video_path, cc_path, panel_version):
    resolved_panel_version = resolve_panel_version(panel_version)
    update_cc_status(cs_id, "GENERATING", panel_version, progress_percentage=0)

    def on_progress(percent):
        if resolved_panel_version != 2:
            return
        update_cc_status(
            cs_id,
            "GENERATING",
            panel_version,
            progress_percentage=percent,
        )

    model = preload_model()
    result = transcribe_with_progress(model, video_path, on_progress=on_progress)
    if not os.path.exists(os.path.dirname(cc_path)):
        os.makedirs(os.path.dirname(cc_path))
    srt_writer = get_writer(subtitle_type, os.path.dirname(cc_path))
    srt_writer(
        result, os.path.basename(cc_path))
    print(f"Subtitles generated for {video_path}")
    update_cc_status(
        cs_id,
        "GENERATED",
        panel_version,
        subtitle_file_path=cc_path,
        progress_percentage=100,
    )


def convert_m3u8_to_m4a(input_file):
    output_file = os.path.splitext(input_file)[0] + ".m4a"
    if os.path.exists(output_file):
        print(f"File {output_file} already exists. Skipping conversion.")
        return output_file
    ffmpeg_cmd = ["ffmpeg", "-i", input_file, "-vn",
                  "-c:a", "aac", "-strict", "-2", output_file]
    subprocess.run(ffmpeg_cmd, check=True)
    print(f"Conversion successful. M4A file saved as {output_file}")
    return output_file


def on_message_callback(ch, method, properties, body):
    acked = False
    should_requeue = False
    parsed_body = None
    panel_version = panel_version_default
    processing_lock_path = None
    processing_lock_acquired = False
    try:
        delivery_tag = getattr(method, "delivery_tag", None)
        redelivered = getattr(method, "redelivered", False)
        print(
            "Received message "
            f"(delivery_tag={delivery_tag}, redelivered={redelivered}, ack_mode={rabbitmq_ack_mode}): "
            + str(body)
        )
        if rabbitmq_ack_mode == "early":
            ch.basic_ack(delivery_tag=method.delivery_tag)
            acked = True
            print(f"Message acknowledged early (delivery_tag={delivery_tag})")

        parsed_body = json.loads(body)
        cs_id = parsed_body["cs_id"]
        content_id = parsed_body["content_id"]
        vod_type = parsed_body["vod_type"]
        cs_path = parsed_body["cs_path"]
        lowest_resolution = parsed_body["lowest_resolution"]
        cc_path = normalize_path(parsed_body["cc_path"])
        user_id = parsed_body["user_id"]
        panel_version = parsed_body.get("panel_version", panel_version_default)

        if subtitle_file_exists_and_non_empty(cc_path):
            print(
                f"Detected existing subtitle output for cs_id={cs_id} at '{cc_path}'. "
                "Skipping transcription for idempotency."
            )
            update_cc_status(
                cs_id,
                "GENERATED",
                panel_version,
                subtitle_file_path=cc_path,
                progress_percentage=100,
            )
            return

        processing_lock_path = build_processing_lock_path(cc_path)
        processing_lock_acquired, lock_reason = acquire_processing_lock(processing_lock_path, cs_id)
        if not processing_lock_acquired:
            print(
                f"Duplicate/in-progress message detected for cs_id={cs_id}. "
                f"lock='{processing_lock_path}', reason={lock_reason}, policy={duplicate_message_policy}"
            )
            if duplicate_message_policy == "requeue" and not acked:
                should_requeue = True
            return

        if vod_type == "ts":
            low_res_m3u8_video_path = f"{storage_path}/{user_id}/{content_id}/{lowest_resolution}p.m3u8"
            generate_cc(cs_id, convert_m3u8_to_m4a(
                low_res_m3u8_video_path), cc_path, panel_version)
        elif vod_type == "mp4":
            low_res_mp4_video_path = f"{storage_path}/{user_id}/{get_filename(cs_path)}_{lowest_resolution}.mp4"
            uploaded_video_path = f"{storage_path}/{user_id}/{cs_path}"
            if os.path.exists(low_res_mp4_video_path):
                generate_cc(cs_id, low_res_mp4_video_path, cc_path, panel_version)
            elif os.path.exists(uploaded_video_path):
                generate_cc(cs_id, uploaded_video_path, cc_path, panel_version)
            else:
                print("Error occured: Video file not found")
                update_cc_status(cs_id, "ERROR", panel_version, error_message="Video file not found")
        else:
            print(f"Error occured: Unsupported vod_type '{vod_type}'")
            update_cc_status(cs_id, "ERROR", panel_version, error_message=f"Unsupported vod_type '{vod_type}'")
    except Exception as e:
        print("Error occured: " + str(e))
        if isinstance(parsed_body, dict) and "cs_id" in parsed_body:
            update_cc_status(
                parsed_body["cs_id"],
                "ERROR",
                parsed_body.get("panel_version", panel_version),
                error_message=format_error(e),
            )
    finally:
        if processing_lock_acquired:
            release_processing_lock(processing_lock_path)

        if not acked:
            if should_requeue:
                print("Requeueing duplicate/in-progress message.")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            else:
                # Fair dispatch: acknowledge only after processing completes.
                ch.basic_ack(delivery_tag=method.delivery_tag)


def start_consuming():
    connection = create_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    # Process one unacked message at a time per consumer for balanced load.
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=queue_name, on_message_callback=on_message_callback, auto_ack=False)
    print(f"Waiting for messages... (queue={queue_name}, ack_mode={rabbitmq_ack_mode})")
    channel.start_consuming()


if __name__ == "__main__":
    try:
        preload_model()
        start_consuming()
    except Exception as e:
        print("Error occured: " + str(e))
