import base64
import json
import logging
from logging import Handler
import socket
import struct
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

TOPIC = "adv/+/data"
HEADER_LEN = 4  # data header length (magic + size + count + reserved)
DATA_STRUCT = struct.Struct("<LiihbbbBB")


class DailyLogFileHandler(Handler):
    """Simple daily log handler that keeps the active file named with the date."""

    def __init__(self, log_dir: Path):
        super().__init__()
        self.log_dir = log_dir
        self.current_date = ""
        self.stream = None
        self._ensure_stream()

    def _log_path_for_date(self, date_str: str) -> Path:
        return self.log_dir / f"l50wialon-{date_str}.log"

    def _ensure_stream(self) -> None:
        date_str = datetime.now().strftime("%Y%m%d")
        if date_str == self.current_date and self.stream:
            return
        if self.stream:
            try:
                self.stream.close()
            except Exception:
                pass
        self.current_date = date_str
        self.stream = self._log_path_for_date(date_str).open("a", encoding="utf-8")

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._ensure_stream()
            msg = self.format(record)
            self.stream.write(msg + "\n")
            self.stream.flush()
        except Exception:
            self.handleError(record)

    def close(self) -> None:
        if self.stream:
            try:
                self.stream.close()
            except Exception:
                pass
            self.stream = None
        super().close()


def get_version() -> str:
    ver_file = Path(__file__).resolve().parent / "version.txt"
    if ver_file.exists():
        return ver_file.read_text().strip()
    return "unknown"


def get_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def load_config(filename: str = "config.json") -> Dict[str, Any]:
    base_dir = get_base_dir()
    cfg_path = base_dir / filename
    with cfg_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def setup_logging() -> Path:
    base_dir = get_base_dir()
    log_dir = base_dir / "log"
    log_dir.mkdir(parents=True, exist_ok=True)

    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    formatter = logging.Formatter(fmt)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    file_handler = DailyLogFileHandler(log_dir)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logging.captureWarnings(True)

    current_log = log_dir / f"l50wialon-{file_handler.current_date}.log"
    logger.debug("Logging initialized; writing to %s", current_log)
    return current_log


def extract_device(topic: str) -> str:
    parts = topic.split("/")
    return parts[1] if len(parts) > 1 else ""


def get_wialon_device_config(imei, devices, default_password):
    """
    Find device config by IMEI.
    - If not found or enabled == False: return (False, None, None)
    - If found and enabled == True:
      - If password is empty or missing: use default_password or "NA"
      - Return (True, imei, password_to_use)
    """
    for dev in devices:
        if dev.get("imei") != imei:
            continue
        if not dev.get("enabled", False):
            return False, None, None
        pwd = dev.get("password") or default_password or "NA"
        return True, imei, pwd
    return False, None, None


def _crc16_a001(data: bytes) -> int:
    crc = 0
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc


def _format_lat_lon_ddmm(value, is_lat) -> Tuple[str, str]:
    """Convert decimal degrees to DDMM.MMMM / DDDMM.MMMM + sign."""
    sign = "N" if value >= 0 else "S"
    if not is_lat:
        sign = "E" if value >= 0 else "W"
    abs_val = abs(value)
    degrees = int(abs_val)
    minutes = (abs_val - degrees) * 60.0
    ddmm = degrees * 100 + minutes
    precision = 4
    return f"{ddmm:.{precision}f}", sign


def build_wialon_extended_d_packet(record, imei):
    # 1) Date/Time in UTC
    dt = datetime.fromtimestamp(record["log_ts"], tz=timezone.utc)
    date_str = dt.strftime("%d%m%y")
    time_str = dt.strftime("%H%M%S")

    # 2) Lat/Lon to DDMM.MMMM / DDDMM.MMMM with signs
    lat_ddmm, lat_sign = _format_lat_lon_ddmm(record["la"], is_lat=True)
    lon_ddmm, lon_sign = _format_lat_lon_ddmm(record["lg"], is_lat=False)

    # 3) Basic numeric fields ? keep as simple numeric/NA defaults
    speed = "0"
    course = "0"
    alt = "0"
    sats = "0"
    hdop = "0"
    inputs = "0"
    outputs = "0"
    adc = "0"
    ibutton = "NA"

    # 4) Build params as NAME:TYPE:VALUE, comma-separated
    params_items = []

    field_specs = [
        ("tmp",   2),  # double
        ("tiltx", 1),  # int
        ("tilty", 1),  # int
        ("tiltz", 1),  # int
        ("corev", 2),  # double
        ("liionv", 2), # double
        ("oper",  3),  # string
        ("ip",    3),  # string
        ("seq",   1),  # int
        ("tx_ts", 1),  # int
        ("remark",3),  # string
    ]

    for name, ptype in field_specs:
        value = record.get(name)
        # skip completely missing values
        if value is None:
            continue

        # string params: ensure non-None string
        if ptype == 3:
            value_str = "" if value is None else str(value)
        else:
            value_str = str(value)

        params_items.append(f"{name}:{ptype}:{value_str}")

    params = ",".join(params_items)

    # 5) Assemble body WITHOUT CRC and WITHOUT param count
    body = (
        f"{date_str};{time_str};"
        f"{lat_ddmm};{lat_sign};"
        f"{lon_ddmm};{lon_sign};"
        f"{speed};{course};{alt};{sats};{hdop};{inputs};{outputs};"
        f"{adc};{ibutton};{params}"
    )

    return f"#D#{body}\r\n"

def _recv_line(sock):
    """
    Read from socket until '\n', return the decoded ASCII string without trailing CR/LF.
    Return None on timeout or error.
    """
    try:
        buf = []
        while True:
            chunk = sock.recv(1)
            if not chunk:
                break
            if chunk == b"\n":
                break
            buf.append(chunk)
        if not buf:
            return None
        line = b"".join(buf).decode("ascii", errors="ignore").rstrip("\r")
        return line
    except Exception as exc:  # pylint: disable=broad-except
        logging.warning("Wialon recv error: %s", exc)
        return None


def send_multiple_wialon_records(imei, password, packets, host, port):
    """
    Send a login and multiple D packets over one TCP session with retries.
    """
    if not host or not port:
        logging.warning("Wialon host or port missing; skipping multi-send")
        return
    if not packets:
        return

    pwd_used = password if password else "NA"
    login_packet = "#L#{imei};{pwd}\r\n".format(imei=imei, pwd=pwd_used)

    for attempt in range(3):
        sock = None
        try:
            sock = socket.create_connection((host, port), timeout=10)
            sock.settimeout(5)
            logging.info("[WIALON-MULTI] TCP connected")

            logging.info("[WIALON-LGN] %r", login_packet)
            sock.sendall(login_packet.encode("ascii"))
            login_resp = _recv_line(sock)
            logging.info("[WIALON-RX-L] %s", login_resp)

            if not login_resp or login_resp.startswith("#AL#0") or not login_resp.startswith("#AL#1"):
                logging.warning("Wialon login failed (attempt %d): %s", attempt + 1, login_resp)
                continue

            for idx, packet in enumerate(packets):
                for pkt_attempt in range(3):
                    logging.info("[WIALON-PKT-%d] %s", idx, packet.strip())
                    sock.sendall(packet.encode("ascii"))
                    data_resp = _recv_line(sock)
                    logging.info("[WIALON-RX-D] %s", data_resp)

                    if data_resp and data_resp.startswith("#AD#1"):
                        break

                    logging.warning(
                        "Wialon ack issue for packet %d (try %d): %s",
                        idx,
                        pkt_attempt + 1,
                        data_resp,
                    )

                    if pkt_attempt == 2:
                        logging.warning("Giving up on packet index %d after retries", idx)
                # continue to next packet regardless of ack result

            logging.info("[WIALON-MULTI] session complete, closing socket")
            return
        except Exception as exc:  # pylint: disable=broad-except
            logging.warning("Wialon multi-send error (attempt %d): %s", attempt + 1, exc)
        finally:
            if sock:
                try:
                    sock.close()
                except Exception:
                    pass

    logging.warning("Wialon multi-send login failed after retries")


def send_one_wialon_cycle(imei, password, packet, host, port, debug_enabled=False):
    """
    Send one Wialon TCP cycle: login then one D packet.
    """
    if not host or not port:
        logging.warning("Wialon host or port missing; skipping send")
        return
    if not packet:
        return

    pwd_used = password if password else "NA"
    # 這裡維持你目前拿到 #AL#1 的格式：IMEI;PWD
    login_packet = "#L#{imei};{pwd}\r\n".format(imei=imei, pwd=pwd_used)

    sock = None
    try:
        sock = socket.create_connection((host, port), timeout=10)
        sock.settimeout(5)

        # send login
        sock.sendall(login_packet.encode("ascii"))
        login_resp = None
        try:
            login_resp_bytes = sock.recv(1024)
            if login_resp_bytes:
                login_resp = login_resp_bytes.decode("ascii", errors="ignore").strip()
        except socket.timeout:
            logging.warning("Wialon login timeout")

        logging.info("[WIALON-LGN] %r", login_packet)
        logging.info("[WIALON-RX-L] %s", login_resp)

        if not login_resp or not login_resp.startswith("#AL#1"):
            logging.warning("Wialon login failed or rejected: %s", login_resp)
            return

        # send data packet
        logging.info("[WIALON-PKT] %s", packet.strip())
        sock.sendall(packet.encode("ascii"))

        data_resp = None
        try:
            data_resp_bytes = sock.recv(1024)
            if data_resp_bytes:
                data_resp = data_resp_bytes.decode("ascii", errors="ignore").strip()
        except socket.timeout:
            logging.warning("Wialon data ack timeout")
        logging.info("[WIALON-RX-D] %s", data_resp)
    except Exception as exc:  # pylint: disable=broad-except
        logging.warning("Wialon send error: %s", exc)
    finally:
        if sock:
            try:
                sock.close()
            except Exception:
                pass


def parse_leo_l50_data_message(raw_msg, topic_device, debug_enabled=False):
    msg = raw_msg.strip()
    try:
        j = json.loads(msg)
    except Exception as exc:  # pylint: disable=broad-except
        logging.warning("failed to decode JSON payload: %s", exc)
        return []

    data_field = j.get("data")
    if not isinstance(data_field, str):
        logging.warning("data field missing or not a string; skipping message")
        return []

    try:
        payload_b64 = data_field
        payload_bytes = base64.b64decode(payload_b64.encode("ascii"))
    except Exception as exc:  # pylint: disable=broad-except
        logging.warning("failed to base64 decode data: %s", exc)
        return []

    if debug_enabled:
        logging.debug("==== LEO-L50 DATA DEBUG ====")
        logging.debug("[DEBUG] device=%s", topic_device)
        logging.debug("[DEBUG] raw JSON msg=%s", msg)
        logging.debug("[DEBUG] payload b64=%s", payload_b64)
        logging.debug("[DEBUG] payload length=%d", len(payload_bytes))
        logging.debug("[DEBUG] payload hex=%s", payload_bytes.hex())

    header_len = HEADER_LEN
    if len(payload_bytes) < header_len:
        logging.warning("data payload too short for header: %d", len(payload_bytes))
        return []

    header = payload_bytes[:header_len]
    magic = header[0]
    size = header[1]
    count = header[2]

    if debug_enabled:
        logging.debug("[DEBUG] header bytes=%s", list(header))
        logging.debug("[DEBUG] header: magic=0x%02X, size=%d, count=%d", magic, size, count)

    if magic != 0xA5:
        logging.warning("unexpected magic byte: 0x%02X", magic)

    if size != DATA_STRUCT.size:
        logging.warning("record size mismatch: header=%d struct=%d", size, DATA_STRUCT.size)

    expected_len = header_len + count * size
    if len(payload_bytes) < expected_len:
        logging.warning(
            "payload shorter than expected: have=%d expected=%d",
            len(payload_bytes),
            expected_len,
        )

    max_records = min(count, max(0, (len(payload_bytes) - header_len) // size))
    records: List[Dict[str, Any]] = []

    for index in range(max_records):
        start = header_len + index * size
        end = start + size
        log_subary = payload_bytes[start:end]

        if len(log_subary) < DATA_STRUCT.size:
            logging.warning("truncated record at index %d", index)
            break

        (
            log_ts,
            la_raw,
            lg_raw,
            tmp_raw,
            tiltx,
            tilty,
            tiltz,
            corev_raw,
            liionv_raw,
        ) = DATA_STRUCT.unpack(log_subary)

        if debug_enabled:
            logging.debug(
                "[DEBUG] record[%d] raw: ts=%s, la_raw=%s, lg_raw=%s, tmp_raw=%s, "
                "tiltx=%s, tilty=%s, tiltz=%s, corev_raw=%s, liionv_raw=%s",
                index,
                log_ts,
                la_raw,
                lg_raw,
                tmp_raw,
                tiltx,
                tilty,
                tiltz,
                corev_raw,
                liionv_raw,
            )

        la = la_raw * 0.000001
        lg = lg_raw * 0.000001
        tmp = tmp_raw * 0.01
        corev = corev_raw * 0.1
        liionv = liionv_raw * 0.1

        if debug_enabled:
            logging.debug(
                "[DEBUG] record[%d] scaled: la=%s, lg=%s, tmp=%s, corev=%s, liionv=%s",
                index,
                la,
                lg,
                tmp,
                corev,
                liionv,
            )

        records.append(
            {
                "log_ts": log_ts,
                "la": la,
                "lg": lg,
                "tmp": tmp,
                "tiltx": tiltx,
                "tilty": tilty,
                "tiltz": tiltz,
                "corev": corev,
                "liionv": liionv,
            }
        )

    return records


def on_connect(client: mqtt.Client, userdata: Dict[str, Any], flags: Dict[str, Any], rc: int) -> None:
    if rc != 0:
        logging.error("MQTT connect failed with rc=%s", rc)
        return
    logging.info("connected to MQTT broker; subscribing to %s", TOPIC)
    client.subscribe(TOPIC)


def on_message(client: mqtt.Client, userdata: Dict[str, Any], msg: mqtt.MQTTMessage) -> None:
    config = userdata.get("config", {})
    print_json = config.get("output", {}).get("print_json", False)
    debug_enabled = config.get("debug", False)
    wialon_cfg = config.get("wialon", {})
    wialon_host = wialon_cfg.get("host")
    wialon_port = wialon_cfg.get("port")
    wialon_default_password = wialon_cfg.get("default_password", "NA")
    wialon_devices = wialon_cfg.get("devices", [])

    topic_device = extract_device(msg.topic)

    try:
        raw_msg = msg.payload.decode("utf-8")
        payload = json.loads(raw_msg)
    except Exception as exc:  # pylint: disable=broad-except
        logging.warning("failed to decode JSON payload: %s", exc)
        return

    msg_block = payload.get("msg", {})
    net_block = payload.get("net", {})
    imei = msg_block.get("imei", topic_device)

    parsed_records = list(parse_leo_l50_data_message(raw_msg, topic_device, debug_enabled))
    records_sorted = sorted(parsed_records, key=lambda r: r["log_ts"])

    if not records_sorted:
        return

    ok, dev_imei, pwd = get_wialon_device_config(imei, wialon_devices, wialon_default_password)
    packets: List[str] = []

    for idx, record in enumerate(records_sorted):
        output = {
            "device": topic_device,
            "imei": imei,
            "log_index": idx,
            "log_ts": record["log_ts"],
            "la": record["la"],
            "lg": record["lg"],
            "tmp": record["tmp"],
            "tiltx": record["tiltx"],
            "tilty": record["tilty"],
            "tiltz": record["tiltz"],
            "corev": record["corev"],
            "liionv": record["liionv"],
            "tx_ts": msg_block.get("ts"),
            "oper": net_block.get("oper"),
            "ip": net_block.get("ip"),
            "seq": msg_block.get("id"),
            "remark": payload.get("remark", ""),
        }

        if ok and wialon_host and wialon_port:
            packet = build_wialon_extended_d_packet(output, dev_imei)
            if debug_enabled:
                logging.debug("[WIALON-PKT] %s", packet.strip())
            packets.append(packet)

        if print_json:
            logging.info("%s", json.dumps(output))

    if ok:
        logging.info("[WIALON-FWD] imei=%s, password=%s", dev_imei, pwd)
        if wialon_host and wialon_port and packets:
            send_multiple_wialon_records(dev_imei, pwd, packets, wialon_host, wialon_port)
        elif not (wialon_host and wialon_port):
            logging.warning("Wialon host/port missing; skipping send")
    else:
        logging.info("[WIALON-SKIP] imei=%s not in forwarding list or disabled", imei)


def configure_client(config: Dict[str, Any]) -> mqtt.Client:
    mqtt_cfg = config.get("mqtt", {})

    client = mqtt.Client()
    client.user_data_set({"config": config})
    client.on_connect = on_connect
    client.on_message = on_message

    username = mqtt_cfg.get("username") or None
    password = mqtt_cfg.get("password") or None
    if username:
        client.username_pw_set(username, password)

    if mqtt_cfg.get("tls_enabled"):
        client.tls_set(
            ca_certs=mqtt_cfg.get("tls_ca") or None,
            certfile=mqtt_cfg.get("tls_client_cert") or None,
            keyfile=mqtt_cfg.get("tls_client_key") or None,
        )

    broker_ip = mqtt_cfg.get("broker_ip", "")
    port = int(mqtt_cfg.get("port", 1883))
    client.connect(broker_ip, port)
    return client


def main() -> None:
    setup_logging()
    logging.info("L50-Wialon Forwarder version: v%s", get_version())

    config = load_config()
    wialon_cfg = config.get("wialon", {})
    wialon_host = wialon_cfg.get("host")
    wialon_port = wialon_cfg.get("port")
    wialon_default_password = wialon_cfg.get("default_password", "NA")
    wialon_devices = wialon_cfg.get("devices", [])
    config["_wialon_loaded"] = {
        "host": wialon_host,
        "port": wialon_port,
        "default_password": wialon_default_password,
        "devices": wialon_devices,
    }
    client = configure_client(config)
    logging.info("starting MQTT loop")
    client.loop_forever()


if __name__ == "__main__":
    main()
