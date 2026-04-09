import whisper
from whisper.utils import get_writer
import subprocess
import pika
import json
import os
import socket
import importlib
import contextlib
import requests
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

queue_name = 'subtitle_gen_queue'
model_type = 'large'
subtitle_type = 'vtt'
models_dir = './models/'
hostname = os.getenv("RABBITMQ_HOST")
rabbitmq_port = int(os.getenv("RABBITMQ_PORT") or 5672)
username = os.getenv("RABBITMQ_USERNAME")
password = os.getenv("RABBITMQ_PASSWORD")
storage_path = os.getenv("STORAGE_PATH", "/data")
panel_version_default = int(os.getenv("PANEL_VERSION", "1"))
panel_v1_base_url = os.getenv("PANEL_V1_BASE_URL", "https://panel.videoon.ly").rstrip("/")
panel_v2_base_url = os.getenv("PANEL_V2_BASE_URL", "https://panelpp.mediatriple.net").rstrip("/")
panel_v2_status_path = os.getenv("PANEL_V2_STATUS_PATH", "/api/encoder/update-subtitle-status").strip()
panel_v2_api_key = os.getenv("PANEL_V2_API_KEY") or os.getenv("API_KEY", "")
if panel_v2_status_path and not panel_v2_status_path.startswith("/"):
    panel_v2_status_path = f"/{panel_v2_status_path}"
panel_base_urls = {
    1: panel_v1_base_url,
    2: panel_v2_base_url,
}
whisper_model = None


def get_rabbitmq_hosts(primary_host):
    return [primary_host]


def format_error(err):
    message = str(err).strip()
    return message if message else repr(err)


def create_rabbitmq_connection():
    if not hostname or not rabbitmq_port or not username or not password:
        raise RuntimeError(
            "RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USERNAME ve RABBITMQ_PASSWORD environment değişkenleri tanımlı olmalı."
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
    parsed_body = None
    panel_version = panel_version_default
    try:
        print("Received message: " + str(body))
        parsed_body = json.loads(body)
        cs_id = parsed_body["cs_id"]
        content_id = parsed_body["content_id"]
        vod_type = parsed_body["vod_type"]
        cs_path = parsed_body["cs_path"]
        lowest_resolution = parsed_body["lowest_resolution"]
        cc_path = parsed_body["cc_path"]
        user_id = parsed_body["user_id"]
        panel_version = parsed_body.get("panel_version", panel_version_default)
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
    print("Waiting for messages...")
    channel.start_consuming()


if __name__ == "__main__":
    try:
        preload_model()
        start_consuming()
    except Exception as e:
        print("Error occured: " + str(e))
