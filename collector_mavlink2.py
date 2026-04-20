import argparse
import csv
import json
import os
import socket
import threading
import time
from collections import deque
from typing import Any, Dict, List, Optional, Tuple

# MAVLink2 dialect (common.xml, v2.0 wire protocol support)
from pymavlink.dialects.v20 import common as mavlink2


def now_s() -> float:
    return time.time()


class RollingCounter:
    """Counter over a sliding time window."""

    def __init__(self, window_s: float):
        self.window_s = float(window_s)
        self.events = deque()

    def add(self, ts: float, value: float = 1.0):
        self.events.append((ts, float(value)))
        self._trim(ts)

    def _trim(self, ts: float):
        cutoff = ts - self.window_s
        while self.events and self.events[0][0] < cutoff:
            self.events.popleft()

    def sum(self, ts: float) -> float:
        self._trim(ts)
        return sum(v for _, v in self.events)

    def rate_per_s(self, ts: float) -> float:
        return self.sum(ts) / max(self.window_s, 1e-9)


class RollingStats:
    """Rolling mean/std over last N points for Z-score anomaly detection."""

    def __init__(self, window_n: int = 400):
        self.window_n = int(window_n)
        self.buf = deque()
        self.s1 = 0.0
        self.s2 = 0.0

    def update(self, x: float):
        x = float(x)
        self.buf.append(x)
        self.s1 += x
        self.s2 += x * x
        if len(self.buf) > self.window_n:
            old = self.buf.popleft()
            self.s1 -= old
            self.s2 -= old * old

    def mean(self) -> float:
        n = len(self.buf)
        return self.s1 / n if n else 0.0

    def std(self) -> float:
        n = len(self.buf)
        if n < 2:
            return 1e-9
        mu = self.mean()
        var = (self.s2 / n) - (mu * mu)
        if var < 1e-12:
            var = 1e-12
        return var**0.5

    def z(self, x: float) -> float:
        return abs(float(x) - self.mean()) / self.std()


def enum_name(enum_group: str, value: Optional[int]) -> str:
    """Best-effort conversion of enum numeric value to a readable name."""
    if value is None:
        return ""
    try:
        enums = getattr(mavlink2, "enums", None)
        if not enums or enum_group not in enums:
            return str(int(value))
        item = enums[enum_group].get(int(value))
        if item is None:
            return str(int(value))
        nm = getattr(item, "name", None)
        return str(nm) if nm else str(int(value))
    except Exception:
        return str(value)


def _ensure_csv_schema(out_csv: str, fieldnames: List[str]):
    """If CSV exists but has a different header, rename it to *.bak_<ts>."""
    if not os.path.exists(out_csv) or os.path.getsize(out_csv) == 0:
        return
    try:
        with open(out_csv, "r", encoding="utf-8") as f:
            first_line = f.readline().strip("\r\n")
        existing = [c.strip() for c in first_line.split(",")]
        if existing != fieldnames:
            bak = f"{out_csv}.bak_{int(now_s())}"
            os.rename(out_csv, bak)
            print(f"[i] CSV schema changed -> renamed old file to: {bak}")
    except Exception:
        pass


def run_collector(
    listen_ip: str,
    port: int,
    window_s: float,
    warmup_s: float,
    out_csv: str,
    z_th: float,
    active_timeout_s: float,
    require_v2: bool,
    primary_sysid_arg: Optional[int],
    debug: bool,
):
    # MAVLink parser 
    parser = mavlink2.MAVLink(None)
    try:
        parser.robust_parsing = True
    except Exception:
        pass

    bytes_counter = RollingCounter(window_s)
    pkts_counter = RollingCounter(window_s)
    msgs_counter = RollingCounter(window_s)
    hb_counter = RollingCounter(window_s)

    # Loss estimation (best-effort using MAVLink seq per (sysid, compid))
    last_seq: Dict[Tuple[int, int], int] = {}
    lost_counter = RollingCounter(window_s)
    recv_seq_counter = RollingCounter(window_s)

    # Filter: lock to one vehicle sysid (prevents mixing multiple systems on one link)
    primary_sysid: Optional[int] = int(primary_sysid_arg) if primary_sysid_arg is not None else None

    # --- Telemetry state ---
    st: Dict[str, Any] = {
        # Heartbeat
        "hb_ts": 0.0,
        "hb_base_mode": None,
        "hb_system_status": None,
        "hb_type": None,
        "hb_autopilot": None,
        "hb_src_sysid": None,
        # GPS
        "gps_ts": 0.0,
        "gps_fix": None,
        "gps_sats": None,
        # Position
        "pos_ts": 0.0,
        "alt_m": None,
        "rel_alt_m": None,
        "speed_m_s": None,
        "vspeed_m_s": None,
        # freshness
        "telem_ts": 0.0,
        # provenance
        "pos_source": "",
        # Attitude (msg #30 ATTITUDE)
        "roll_deg": None,
        "pitch_deg": None,
        "yaw_deg": None,
        # Ground speed (VFR_HUD msg #74)
        "gndspd_m_s": None,
        # RSSI (RC_CHANNELS_RAW #35 or RC_CHANNELS #65)
        "rssi": None,
        # EKF status (msg #193 EKF_STATUS_REPORT)
        "ekf_ts": 0.0,
        "ekf_flags": None,
        # EKF status flags (None = not received yet, 1 = OK, 0 = fault)
        "ekf_vel_ok":       None,
        "ekf_pos_horiz_ok": None,
        "ekf_pos_vert_ok":  None,
        "ekf_compass_ok":   None,
        "ekf_terrain_ok":   None,
        # EKF variances
        "ekf_vel_var": None,
        "ekf_pos_horiz_var": None,
        "ekf_pos_vert_var": None,
        "ekf_compass_var": None,
        "ekf_terrain_alt_var": None,
        # EKF previous good flags (for desync detection)
        "_ekf_prev_vel_ok": None,
        "_ekf_prev_pos_h_ok": None,
        "_ekf_prev_pos_v_ok": None,
        "_ekf_prev_compass_ok": None,
        "_ekf_prev_terrain_ok": None,
    }

    load_stats = RollingStats(window_n=400)
    start_ts = now_s()

    os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)

    CSV_FIELDS = [
        "ts",
        # traffic / anomaly
        "bps",
        "pps",
        "mps",
        "loss_ratio",
        "score",
        "anomaly",
        # heartbeat / drone status
        "primary_sysid",
        "hb_hz",
        "hb_age_s",
        "drone_active",
        "armed",
        "mav_state",
        "mav_state_name",
        "mav_type",
        "mav_type_name",
        "autopilot",
        "autopilot_name",
        # GPS
        "gps_fix",
        "gps_fix_name",
        "gps_sats",
        "gps_age_s",
        # position
        "alt_m",
        "rel_alt_m",
        "speed_m_s",
        "vspeed_m_s",
        "pos_age_s",
        "pos_source",
        # attitude (ATTITUDE msg #30)
        "roll_deg",
        "pitch_deg",
        "yaw_deg",
        # ground speed (VFR_HUD msg #74)
        "gndspd_m_s",
        # RSSI (RC_CHANNELS_RAW #35 or RC_CHANNELS #65)
        "rssi",
        # EKF status flags (EKF_STATUS_REPORT msg #193)
        "ekf_flags",
        "ekf_vel_ok",        # velocity horiz variance OK
        "ekf_pos_horiz_ok",  # position horizontal variance OK
        "ekf_pos_vert_ok",   # position vertical variance OK
        "ekf_compass_ok",    # compass variance OK
        "ekf_terrain_ok",    # terrain alt variance OK
        # EKF variances (float, lower = better)
        "ekf_vel_var",
        "ekf_pos_horiz_var",
        "ekf_pos_vert_var",
        "ekf_compass_var",
        "ekf_terrain_alt_var",
        # EKF desync anomaly (derived)
        "ekf_desync",        # 1 if any EKF flag drops while drone is active
        # misc
        "telem_age_s",
    ]

    _ensure_csv_schema(out_csv, CSV_FIELDS)

    csv_lock = threading.Lock()
    wrote_header = os.path.exists(out_csv) and os.path.getsize(out_csv) > 0

    def write_row(row: Dict[str, Any]):
        nonlocal wrote_header
        with csv_lock:
            with open(out_csv, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
                if not wrote_header:
                    w.writeheader()
                    wrote_header = True
                w.writerow(row)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((listen_ip, port))
    sock.settimeout(0.5)

    print(f"[+] Listening UDP on {listen_ip}:{port}")
    print(
        f"[+] Window={window_s}s warmup={warmup_s}s CSV={out_csv} | Z-threshold={z_th} | active_timeout={active_timeout_s}s | require_v2={require_v2}"
    )
    if primary_sysid is not None:
        print(f"[+] Filtering on sysid={primary_sysid}")
    else:
        print("[+] Filtering: auto-lock to first HEARTBEAT sysid")
    print("[+] Press Ctrl+C to stop.")

    ARM_FLAG = getattr(mavlink2, "MAV_MODE_FLAG_SAFETY_ARMED", 128)
    MAVLINK2_MAGIC = 0xFD

    def accept_msg_for_vehicle(sysid: int) -> bool:
        if primary_sysid is None:
            return True
        return sysid == primary_sysid

    def feed_bytes(ts: float, data: bytes):
        nonlocal primary_sysid

        pkts_counter.add(ts, 1)
        bytes_counter.add(ts, len(data))

        for b in data:
            msg = parser.parse_char(bytes([b]))
            if msg is None:
                continue

            msgs_counter.add(ts, 1)

            # Enforce MAVLink2 only if requested
            if require_v2:
                hdr = getattr(msg, "_header", None)
                magic = getattr(hdr, "magic", None) if hdr is not None else None
                if magic != MAVLINK2_MAGIC:
                    continue

            mid = msg.get_msgId()
            sysid = msg.get_srcSystem()
            compid = msg.get_srcComponent()

            # Auto lock primary sysid based on first heartbeat
            if mid == 0 and primary_sysid is None:
                primary_sysid = int(sysid)
                print(f"[i] Auto-locked primary sysid={primary_sysid} (first HEARTBEAT)")

            if not accept_msg_for_vehicle(sysid):
                continue

            # HEARTBEAT
            if mid == 0:
                st["hb_base_mode"] = getattr(msg, "base_mode", None)
                st["hb_system_status"] = getattr(msg, "system_status", None)
                st["hb_type"] = getattr(msg, "type", None)
                st["hb_autopilot"] = getattr(msg, "autopilot", None)
                st["hb_src_sysid"] = int(sysid)
                st["hb_ts"] = ts
                st["telem_ts"] = ts
                hb_counter.add(ts, 1)

            # GPS_RAW_INT
            elif mid == 24:
                try:
                    fx = getattr(msg, "fix_type", None)
                    sv = getattr(msg, "satellites_visible", None)

                    if fx is not None:
                        st["gps_fix"] = int(fx)

                    if sv is not None:
                        svi = int(sv)
                        # UINT8_MAX (255) means unknown
                        if svi != 255:
                            st["gps_sats"] = svi

                    st["gps_ts"] = ts
                    st["telem_ts"] = ts
                except Exception:
                    pass

            # GPS2_RAW (#124) — secondary GPS, use if primary hasn't given sats yet
            elif mid == 124:
                try:
                    sv2 = getattr(msg, "satellites_visible", None)
                    if sv2 is not None and st.get("gps_sats") is None:
                        svi2 = int(sv2)
                        if svi2 != 255:
                            st["gps_sats"] = svi2
                    st["telem_ts"] = ts
                except Exception:
                    pass

            # GLOBAL_POSITION_INT
            elif mid == 33:
                try:
                    alt_mm = getattr(msg, "alt", None)
                    rel_mm = getattr(msg, "relative_alt", None)
                    if alt_mm is not None:
                        st["alt_m"] = float(alt_mm) / 1000.0
                    if rel_mm is not None:
                        st["rel_alt_m"] = float(rel_mm) / 1000.0

                    vx = getattr(msg, "vx", None)  # cm/s
                    vy = getattr(msg, "vy", None)
                    vz = getattr(msg, "vz", None)
                    if vx is not None and vy is not None:
                        st["speed_m_s"] = ((float(vx) ** 2 + float(vy) ** 2) ** 0.5) / 100.0
                    if vz is not None:
                        # NED: positive down -> convert to "up positive"
                        st["vspeed_m_s"] = -float(vz) / 100.0

                    st["pos_ts"] = ts
                    st["telem_ts"] = ts
                    st["pos_source"] = "GLOBAL_POSITION_INT"
                except Exception:
                    pass

            # VFR_HUD (fallback)
            elif mid == 74:
                try:
                    alt = getattr(msg, "alt", None)  # m
                    if alt is not None:
                        st["alt_m"] = float(alt)

                    gs = getattr(msg, "groundspeed", None)  # m/s
                    if gs is not None:
                        st["speed_m_s"] = float(gs)
                        st["gndspd_m_s"] = float(gs)

                    cl = getattr(msg, "climb", None)  # m/s
                    if cl is not None:
                        st["vspeed_m_s"] = float(cl)

                    st["pos_ts"] = ts
                    st["telem_ts"] = ts
                    if not st.get("pos_source"):
                        st["pos_source"] = "VFR_HUD"
                except Exception:
                    pass

            # ATTITUDE (#30) — roll, pitch, yaw in radians → convert to degrees
            elif mid == 30:
                try:
                    import math
                    roll = getattr(msg, "roll", None)
                    pitch = getattr(msg, "pitch", None)
                    yaw = getattr(msg, "yaw", None)
                    if roll is not None:
                        st["roll_deg"] = round(math.degrees(float(roll)), 2)
                    if pitch is not None:
                        st["pitch_deg"] = round(math.degrees(float(pitch)), 2)
                    if yaw is not None:
                        st["yaw_deg"] = round(math.degrees(float(yaw)), 2)
                    st["telem_ts"] = ts
                except Exception:
                    pass

            # RC_CHANNELS_RAW (#35) — RSSI field (0-255, 255 = unknown)
            elif mid == 35:
                try:
                    rssi_val = getattr(msg, "rssi", None)
                    if rssi_val is not None:
                        iv = int(rssi_val)
                        if iv != 255:
                            st["rssi"] = iv
                    st["telem_ts"] = ts
                except Exception:
                    pass

            # RC_CHANNELS (#65) — also carries RSSI (0-254, 255 = unknown)
            elif mid == 65:
                try:
                    rssi_val = getattr(msg, "rssi", None)
                    if rssi_val is not None:
                        iv = int(rssi_val)
                        if iv != 255:
                            st["rssi"] = iv
                    st["telem_ts"] = ts
                except Exception:
                    pass

            # RADIO_STATUS (#109) — remrssi = remote RSSI (best source for RSSI)
            elif mid == 109:
                try:
                    remrssi = getattr(msg, "remrssi", None)
                    rssi_val = getattr(msg, "rssi", None)
                    # Prefer remrssi (drone-side), fallback to rssi (GCS-side)
                    best = remrssi if remrssi is not None else rssi_val
                    if best is not None:
                        iv = int(best)
                        if iv != 255:
                            st["rssi"] = iv
                    st["telem_ts"] = ts
                except Exception:
                    pass

            # SYS_STATUS (#1) — onboard_control_sensors fields, not RSSI but useful fallback
            # Actually check for drop_rate_comm as link quality proxy when no RSSI available
            elif mid == 1:
                try:
                    drop = getattr(msg, "drop_rate_comm", None)
                    if drop is not None and st.get("rssi") is None:
                        # Convert drop rate (0-10000 = 0-100%) to RSSI-like scale (255-0)
                        # Just store it as a quality indicator if no real RSSI available
                        pass  # keep for future use
                    st["telem_ts"] = ts
                except Exception:
                    pass

            # EKF_STATUS_REPORT (#193)
            # flags bitmask (EKF_STATUS_FLAGS):
            #   bit0 = EKF_ATTITUDE, bit1 = EKF_VELOCITY_HORIZ, bit2 = EKF_VELOCITY_VERT,
            #   bit3 = EKF_POS_HORIZ_REL, bit4 = EKF_POS_HORIZ_ABS,
            #   bit5 = EKF_POS_VERT_ABS, bit6 = EKF_POS_VERT_AGL,
            #   bit7 = EKF_CONST_POS_MODE, bit8 = EKF_PRED_POS_HORIZ_REL,
            #   bit9 = EKF_PRED_POS_HORIZ_ABS, bit10 = EKF_UNINITIALIZED
            elif mid == 193:
                try:
                    flags = getattr(msg, "flags", None)
                    vel_var      = getattr(msg, "velocity_variance",    None)
                    pos_h_var    = getattr(msg, "pos_horiz_variance",   None)
                    pos_v_var    = getattr(msg, "pos_vert_variance",    None)
                    compass_var  = getattr(msg, "compass_variance",     None)
                    terrain_var  = getattr(msg, "terrain_alt_variance", None)

                    if flags is not None:
                        f = int(flags)
                        st["ekf_flags"] = f
                        # velocity horiz OK: bit1
                        st["ekf_vel_ok"]       = 1 if (f & (1 << 1)) else 0
                        # pos horiz OK: bit3 OR bit4
                        st["ekf_pos_horiz_ok"] = 1 if (f & ((1 << 3) | (1 << 4))) else 0
                        # pos vert OK: bit5 OR bit6
                        st["ekf_pos_vert_ok"]  = 1 if (f & ((1 << 5) | (1 << 6))) else 0
                        # compass OK: bit0 (attitude) as proxy (no dedicated compass bit)
                        st["ekf_compass_ok"]   = 1 if (f & (1 << 0)) else 0
                        # terrain OK: bit6 (EKF_POS_VERT_AGL)
                        st["ekf_terrain_ok"]   = 1 if (f & (1 << 6)) else 0

                    if vel_var     is not None: st["ekf_vel_var"]         = float(vel_var)
                    if pos_h_var   is not None: st["ekf_pos_horiz_var"]   = float(pos_h_var)
                    if pos_v_var   is not None: st["ekf_pos_vert_var"]    = float(pos_v_var)
                    if compass_var is not None: st["ekf_compass_var"]     = float(compass_var)
                    if terrain_var is not None: st["ekf_terrain_alt_var"] = float(terrain_var)

                    st["ekf_ts"] = ts
                    st["telem_ts"] = ts
                except Exception:
                    pass

            # Sequence loss estimation (vehicle sysid only)
            try:
                hdr = getattr(msg, "_header", None)
                seq = hdr.seq if hdr is not None and hasattr(hdr, "seq") else None
                if seq is not None:
                    key = (int(sysid), int(compid))
                    recv_seq_counter.add(ts, 1)
                    if key in last_seq:
                        prev = last_seq[key]
                        exp = (prev + 1) & 0xFF
                        if seq != exp:
                            gap = (seq - exp) & 0xFF
                            if gap > 0:
                                lost_counter.add(ts, gap)
                    last_seq[key] = int(seq)
            except Exception:
                pass

            if debug:
                try:
                    print(f"[dbg] sys={sysid} comp={compid} msg={msg.get_type()}({mid})")
                except Exception:
                    pass

    last_emit = 0.0

    while True:
        ts = now_s()
        try:
            data, _addr = sock.recvfrom(8192)
            feed_bytes(ts, data)
        except socket.timeout:
            pass

        if ts - last_emit >= 1.0:
            last_emit = ts

            bps = bytes_counter.rate_per_s(ts) * 8.0
            pps = pkts_counter.rate_per_s(ts)
            mps = msgs_counter.rate_per_s(ts)

            lost = lost_counter.sum(ts)
            recvseq = recv_seq_counter.sum(ts)
            loss_ratio = (lost / (lost + recvseq)) if (lost + recvseq) > 0 else 0.0

            load = (bps / 1e5) + (pps / 200.0) + (mps / 200.0) + (loss_ratio * 10.0)
            load_stats.update(load)

            elapsed = ts - start_ts
            if elapsed <= warmup_s or len(load_stats.buf) < 30:
                score = 0.0
                is_anom = 0
            else:
                score = load_stats.z(load)
                is_anom = 1 if score >= z_th else 0

            hb_ts = float(st.get("hb_ts") or 0.0)
            hb_age_s = (ts - hb_ts) if hb_ts > 0 else None
            hb_hz = hb_counter.rate_per_s(ts)
            drone_active = 1 if (hb_age_s is not None and hb_age_s <= active_timeout_s) else 0

            armed = 0
            if st.get("hb_base_mode") is not None:
                try:
                    armed = 1 if (int(st["hb_base_mode"]) & int(ARM_FLAG)) != 0 else 0
                except Exception:
                    armed = 0

            gps_ts = float(st.get("gps_ts") or 0.0)
            gps_age_s = (ts - gps_ts) if gps_ts > 0 else None

            pos_ts = float(st.get("pos_ts") or 0.0)
            pos_age_s = (ts - pos_ts) if pos_ts > 0 else None

            telem_ts = float(st.get("telem_ts") or 0.0)
            telem_age_s = (ts - telem_ts) if telem_ts > 0 else None

            mav_state_val = st.get("hb_system_status")
            mav_type_val = st.get("hb_type")
            autopilot_val = st.get("hb_autopilot")

            # ── EKF desync detection ─────────────────────────────────────────
            # A flag that was OK (1) on the previous second and is now 0 (lost)
            # while the drone is still active = EKF desynchronisation event.
            ekf_desync = 0
            if drone_active:
                for cur_key, prev_key in [
                    ("ekf_vel_ok",       "_ekf_prev_vel_ok"),
                    ("ekf_pos_horiz_ok", "_ekf_prev_pos_h_ok"),
                    ("ekf_pos_vert_ok",  "_ekf_prev_pos_v_ok"),
                    ("ekf_compass_ok",   "_ekf_prev_compass_ok"),
                    ("ekf_terrain_ok",   "_ekf_prev_terrain_ok"),
                ]:
                    cur  = st.get(cur_key)
                    prev = st.get(prev_key)
                    if prev == 1 and cur == 0:
                        ekf_desync = 1
                    if cur is not None:
                        st[prev_key] = cur

            row = {
                "ts": int(ts),
                "bps": float(bps),
                "pps": float(pps),
                "mps": float(mps),
                "loss_ratio": float(loss_ratio),
                "score": float(score),
                "anomaly": int(is_anom),

                "primary_sysid": int(primary_sysid) if primary_sysid is not None else "",
                "hb_hz": float(hb_hz),
                "hb_age_s": hb_age_s if hb_age_s is not None else "",
                "drone_active": int(drone_active),
                "armed": int(armed),
                "mav_state": mav_state_val if mav_state_val is not None else "",
                "mav_state_name": enum_name("MAV_STATE", mav_state_val),
                "mav_type": mav_type_val if mav_type_val is not None else "",
                "mav_type_name": enum_name("MAV_TYPE", mav_type_val),
                "autopilot": autopilot_val if autopilot_val is not None else "",
                "autopilot_name": enum_name("MAV_AUTOPILOT", autopilot_val),

                "gps_fix": st.get("gps_fix") if st.get("gps_fix") is not None else "",
                "gps_fix_name": enum_name("GPS_FIX_TYPE", st.get("gps_fix")),
                "gps_sats": st.get("gps_sats") if st.get("gps_sats") is not None else "",
                "gps_age_s": gps_age_s if gps_age_s is not None else "",

                "alt_m": st.get("alt_m") if st.get("alt_m") is not None else "",
                "rel_alt_m": st.get("rel_alt_m") if st.get("rel_alt_m") is not None else "",
                "speed_m_s": st.get("speed_m_s") if st.get("speed_m_s") is not None else "",
                "vspeed_m_s": st.get("vspeed_m_s") if st.get("vspeed_m_s") is not None else "",
                "pos_age_s": pos_age_s if pos_age_s is not None else "",
                "pos_source": st.get("pos_source") or "",

                # attitude
                "roll_deg":    st.get("roll_deg")    if st.get("roll_deg")    is not None else "",
                "pitch_deg":   st.get("pitch_deg")   if st.get("pitch_deg")   is not None else "",
                "yaw_deg":     st.get("yaw_deg")     if st.get("yaw_deg")     is not None else "",
                # ground speed / RSSI
                "gndspd_m_s":  st.get("gndspd_m_s")  if st.get("gndspd_m_s")  is not None else "",
                "rssi":        st.get("rssi")         if st.get("rssi")         is not None else "",
                # EKF flags
                "ekf_flags":        st.get("ekf_flags")        if st.get("ekf_flags")        is not None else "",
                "ekf_vel_ok":       st.get("ekf_vel_ok")       if st.get("ekf_vel_ok")       is not None else "",
                "ekf_pos_horiz_ok": st.get("ekf_pos_horiz_ok") if st.get("ekf_pos_horiz_ok") is not None else "",
                "ekf_pos_vert_ok":  st.get("ekf_pos_vert_ok")  if st.get("ekf_pos_vert_ok")  is not None else "",
                "ekf_compass_ok":   st.get("ekf_compass_ok")   if st.get("ekf_compass_ok")   is not None else "",
                "ekf_terrain_ok":   st.get("ekf_terrain_ok")   if st.get("ekf_terrain_ok")   is not None else "",
                # EKF variances
                "ekf_vel_var":         st.get("ekf_vel_var")         if st.get("ekf_vel_var")         is not None else "",
                "ekf_pos_horiz_var":   st.get("ekf_pos_horiz_var")   if st.get("ekf_pos_horiz_var")   is not None else "",
                "ekf_pos_vert_var":    st.get("ekf_pos_vert_var")    if st.get("ekf_pos_vert_var")    is not None else "",
                "ekf_compass_var":     st.get("ekf_compass_var")     if st.get("ekf_compass_var")     is not None else "",
                "ekf_terrain_alt_var": st.get("ekf_terrain_alt_var") if st.get("ekf_terrain_alt_var") is not None else "",
                # EKF desync
                "ekf_desync": int(ekf_desync),

                "telem_age_s": telem_age_s if telem_age_s is not None else "",
            }

            write_row(row)

            # ── Atomic JSON live state (for dashboard streaming) ────
            try:
                json_path = os.path.join(os.path.dirname(out_csv) or "out", "live_mav.json")
                tmp = json_path + ".tmp"
                os.makedirs(os.path.dirname(json_path) or ".", exist_ok=True)
                # Only serialise JSON-safe values
                json_row = {k: (v if isinstance(v, (int, float, str, bool, type(None))) else str(v))
                            for k, v in row.items()}
                with open(tmp, "w", encoding="utf-8") as fh:
                    json.dump(json_row, fh)
                os.replace(tmp, json_path)
            except Exception:
                pass

            flag = "🚨" if is_anom else " "
            ekf_flag  = " ⚡EKF-DESYNC" if ekf_desync else ""
            alt_txt   = f" alt={st['alt_m']:.1f}m"        if isinstance(st.get("alt_m"),      (float, int)) else " alt=?"
            spd_txt   = f" spd={st['speed_m_s']:.1f}m/s"  if isinstance(st.get("speed_m_s"),  (float, int)) else " spd=?"
            rssi_txt  = f" rssi={st['rssi']}"              if st.get("rssi") is not None        else ""
            sys_txt   = f" sysid={primary_sysid}"          if primary_sysid is not None         else " sysid=?"
            roll_txt  = f" roll={st['roll_deg']:.1f}°"     if isinstance(st.get("roll_deg"),   (float, int)) else ""
            pitch_txt = f" pitch={st['pitch_deg']:.1f}°"   if isinstance(st.get("pitch_deg"),  (float, int)) else ""
            yaw_txt   = f" yaw={st['yaw_deg']:.1f}°"       if isinstance(st.get("yaw_deg"),    (float, int)) else ""
            print(
                f"{flag} bps={bps:10.0f} pps={pps:6.1f} loss={loss_ratio:5.2%} score={score:0.2f} | "
                f"hb={hb_hz:.1f}Hz{sys_txt}{alt_txt}{spd_txt}{rssi_txt}{roll_txt}{pitch_txt}{yaw_txt}{ekf_flag}"
            )


def main():
    ap = argparse.ArgumentParser(
        description="MAVLink UDP collector (Windows) - traffic/anomaly + heartbeat + GPS + position (NO RC, NO battery, NO MAVLink2%)."
    )
    ap.add_argument("--listen", default="0.0.0.0", help="IP to bind (Windows: usually 0.0.0.0)")
    ap.add_argument("--port", type=int, required=True, help="UDP port to listen (example: 14550)")
    ap.add_argument("--window", type=float, default=1.0, help="Rolling window in seconds")
    ap.add_argument("--warmup", type=float, default=0.0, help="Warmup seconds before anomaly detection")
    ap.add_argument("--out", default="out/telemetry_metrics.csv", help="CSV output path")
    ap.add_argument("--z", type=float, default=4.0, help="Z-score threshold")
    ap.add_argument(
        "--active_timeout",
        type=float,
        default=2.0,
        help="Drone considered 'active' if a HEARTBEAT was received within this many seconds.",
    )
    ap.add_argument(
        "--require_v2",
        action="store_true",
        help="If set, ignore MAVLink1 frames (keep MAVLink2 frames only).",
    )
    ap.add_argument(
        "--sysid",
        type=int,
        default=None,
        help="Optional: force filtering on a specific vehicle sysid (otherwise auto-lock on first HEARTBEAT).",
    )
    ap.add_argument(
        "--debug",
        action="store_true",
        help="Debug: print decoded message type (can be very verbose).",
    )

    args = ap.parse_args()

    run_collector(
        listen_ip=args.listen,
        port=args.port,
        window_s=args.window,
        warmup_s=args.warmup,
        out_csv=args.out,
        z_th=args.z,
        active_timeout_s=args.active_timeout,
        require_v2=bool(args.require_v2),
        primary_sysid_arg=args.sysid,
        debug=bool(args.debug),
    )


if __name__ == "__main__":
    main()