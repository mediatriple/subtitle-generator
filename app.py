import whisper
from whisper.utils import get_writer
import subprocess
import pika
import json
import os
import socket
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


def update_cc_status(cs_id, status): return requests.get(
    f"https://panel.videoon.ly/encoders/update_cc_status/{cs_id}/{status}", verify=False)


def get_filename(path): return os.path.splitext(os.path.basename(path))[0]


def preload_model():
    global whisper_model
    if whisper_model is None:
        os.makedirs(models_dir, exist_ok=True)
        print(f"Loading Whisper model '{model_type}' to '{models_dir}'...")
        whisper_model = whisper.load_model(model_type, download_root=models_dir)
        print(f"Whisper model '{model_type}' is ready.")
    return whisper_model


def generate_cc(cs_id, video_path, cc_path):
    update_cc_status(cs_id, "GENERATING")
    model = preload_model()
    result = model.transcribe(
        video_path, language="en", task="translate", fp16=False)
    if not os.path.exists(os.path.dirname(cc_path)):
        os.makedirs(os.path.dirname(cc_path))
    srt_writer = get_writer(subtitle_type, os.path.dirname(cc_path))
    srt_writer(
        result, os.path.basename(cc_path))
    print(f"Subtitles generated for {video_path}")
    update_cc_status(cs_id, "GENERATED")


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
        if vod_type == "ts":
            low_res_m3u8_video_path = f"{storage_path}/{user_id}/{content_id}/{lowest_resolution}p.m3u8"
            generate_cc(cs_id, convert_m3u8_to_m4a(
                low_res_m3u8_video_path), cc_path)
        elif vod_type == "mp4":
            low_res_mp4_video_path = f"{storage_path}/{user_id}/{get_filename(cs_path)}_{lowest_resolution}.mp4"
            uploaded_video_path = f"{storage_path}/{user_id}/{cs_path}"
            if os.path.exists(low_res_mp4_video_path):
                generate_cc(cs_id, low_res_mp4_video_path, cc_path)
            elif os.path.exists(uploaded_video_path):
                generate_cc(cs_id, uploaded_video_path, cc_path)
            else:
                print("Error occured: Video file not found")
                update_cc_status(cs_id, "ERROR")
        else:
            print(f"Error occured: Unsupported vod_type '{vod_type}'")
            update_cc_status(cs_id, "ERROR")
    except Exception as e:
        print("Error occured: " + str(e))
        if isinstance(parsed_body, dict) and "cs_id" in parsed_body:
            update_cc_status(parsed_body["cs_id"], "ERROR")
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
