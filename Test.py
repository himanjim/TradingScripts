"""
HR Exam Screen Capture Tool
Captures the screen every 5 seconds during an exam session.

Uses DXGI Desktop Duplication (dxcam) to bypass SEB's GDI/BitBlt block.

Key fixes vs prior versions
────────────────────────────
1. Singleton eviction  — dxcam.create() is a factory: it silently returns the
   stale (dead) instance if the (device_idx, output_idx) key is still in its
   internal _cameras dict.  We purge that entry BEFORE recreating, so the
   factory actually constructs a fresh DXGI session.

2. DXGI handle release — camera.release() (new dxcam API) frees the underlying
   IDXGIOutputDuplication handle.  Without it, Windows may refuse to hand out a
   new duplication session to the same process.

3. Warm-up loop        — the first few grab() calls on a brand-new DXGI session
   return None while the pipeline primes.  We retry up to N times with a short
   sleep before giving up on a frame.

Requirements:
    pip install dxcam Pillow

Usage:
    python hr_exam_capture.py [--interval 5] [--output ./captures] [--session "Intern_Name"]
"""

import argparse
import time
import signal
import sys
import threading
from datetime import datetime
from pathlib import Path

try:
    import dxcam
    from PIL import Image
except ImportError:
    print("Missing dependencies. Please run:")
    print("  pip install dxcam Pillow")
    sys.exit(1)


# ── Config ───────────────────────────────────────────────────────────────────
DEFAULT_INTERVAL_SECONDS = 5
DEFAULT_OUTPUT_DIR        = r"C:\Users\himan\Documents\whatsapp_meeting_screenshots"
GRAB_TIMEOUT_SECONDS      = 4     # hang detection threshold
WARMUP_RETRIES            = 10    # attempts after camera recreation before giving up
WARMUP_SLEEP              = 0.3   # seconds between warmup retries


# ── Graceful shutdown ────────────────────────────────────────────────────────
_running = True

def _handle_signal(sig, frame):
    global _running
    print("\n[INFO] Capture stopped by user.")
    _running = False

signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ── dxcam singleton helpers ──────────────────────────────────────────────────
def _purge_dxcam_singleton(output_idx: int):
    """
    dxcam.create() is a factory that caches instances in a dict like:
        factory._cameras[(device_idx, output_idx)] = camera
    If we don't remove the dead entry, dxcam.create() returns the same
    stale object every time and prints the "Returning the existed instance"
    warning.  We find the factory by inspecting dxcam's module namespace
    for an object that owns a _cameras dict, then pop the stale key.
    """
    for attr_val in vars(dxcam).values():
        cam_registry = getattr(attr_val, '_cameras', None)
        if isinstance(cam_registry, dict):
            removed = cam_registry.pop((0, output_idx), None)
            if removed is not None:
                return True   # successfully evicted
    return False


def _release_camera(cam):
    """Call cam.release() if available (newer dxcam), else just delete."""
    try:
        cam.release()   # frees IDXGIOutputDuplication handle
    except Exception:
        pass
    try:
        del cam
    except Exception:
        pass


# ── Camera lifecycle ─────────────────────────────────────────────────────────
def _create_camera(output_idx: int):
    return dxcam.create(device_idx=0, output_idx=output_idx, output_color="RGB")


def build_cameras():
    """
    Probe display indices 0, 1, 2 … until dxcam.create() raises.
    Returns list of dicts: { "mon": int, "cam": DXCamera, "idx": int }
    """
    cameras, output_idx = [], 0
    while True:
        try:
            cam = _create_camera(output_idx)
            cameras.append({"mon": output_idx + 1, "cam": cam, "idx": output_idx})
            output_idx += 1
        except Exception:
            break

    if not cameras:
        print("[ERROR] No displays could be opened. Exiting.")
        sys.exit(1)

    print(f"[INFO] Detected {len(cameras)} display(s).")
    return cameras


def recreate_camera(entry: dict) -> bool:
    """
    Properly destroy the stale camera, evict it from dxcam's singleton
    registry, then build and warm up a fresh one.
    Returns True if the new camera is ready, False otherwise.
    """
    output_idx = entry["idx"]
    mon        = entry["mon"]

    # 1. Release DXGI handle and delete Python object
    _release_camera(entry.get("cam"))
    entry["cam"] = None

    # 2. Evict from dxcam's factory cache so dxcam.create() builds a real new one
    evicted = _purge_dxcam_singleton(output_idx)
    if not evicted:
        print(f"[WARN] Display {mon}: could not evict singleton — "
              f"recreation may return the stale instance.")

    # 3. Brief pause so Windows can tear down the old duplication session
    time.sleep(1.0)

    # 4. Create fresh camera
    try:
        entry["cam"] = _create_camera(output_idx)
    except Exception as exc:
        print(f"[WARN] Display {mon}: camera creation failed — {exc}")
        return False

    # 5. Warm-up: the new DXGI session returns None for a few frames while
    #    the pipeline initialises.  Retry until we get a real frame.
    print(f"[INFO] Display {mon}: warming up new DXGI session …")
    for attempt in range(WARMUP_RETRIES):
        try:
            frame = entry["cam"].grab()
            if frame is not None:
                print(f"[INFO] Display {mon}: camera ready after "
                      f"{attempt + 1} warmup attempt(s).")
                return True
        except Exception:
            pass
        time.sleep(WARMUP_SLEEP)

    print(f"[WARN] Display {mon}: warmup finished but no frame yet — "
          f"will keep retrying each capture cycle.")
    return True   # camera exists; may still need one more cycle to prime


# ── Per-grab timeout ─────────────────────────────────────────────────────────
def _grab_with_timeout(cam, timeout: float):
    """
    Run cam.grab() in a daemon thread so a blocked DXGI call doesn't
    freeze the main loop.  Returns (frame, timed_out).
    """
    result = [None]
    done   = threading.Event()

    def _worker():
        try:
            result[0] = cam.grab()
        except Exception:
            pass
        finally:
            done.set()

    threading.Thread(target=_worker, daemon=True).start()
    completed = done.wait(timeout=timeout)
    return result[0], not completed


# ── Capture loop ─────────────────────────────────────────────────────────────
def capture_all_monitors(cameras, session_dir: Path, index: int):
    saved = []
    ts    = datetime.now().strftime("%H%M%S_%f")[:12]

    for entry in cameras:
        mon_idx = entry["mon"]
        cam     = entry.get("cam")

        if cam is None:
            # Previous recreation failed; try again this cycle
            recreate_camera(entry)
            cam = entry.get("cam")
            if cam is None:
                continue

        frame, timed_out = _grab_with_timeout(cam, timeout=GRAB_TIMEOUT_SECONDS)

        if timed_out:
            print(f"[WARN] Display {mon_idx}: grab() timed out — "
                  f"DXGI session lost. Recreating camera …")
            recreate_camera(entry)
            continue

        if frame is None:
            # No new frame yet — one more try (handles post-warmup edge case)
            frame, timed_out = _grab_with_timeout(cam, timeout=GRAB_TIMEOUT_SECONDS)
            if timed_out:
                print(f"[WARN] Display {mon_idx}: retry timed out. Recreating …")
                recreate_camera(entry)
                continue
            if frame is None:
                print(f"[WARN] Display {mon_idx}: frame still None — "
                      f"will retry next cycle.")
                continue

        img      = Image.fromarray(frame, mode="RGB")
        filename = f"cap_{index:05d}_mon{mon_idx}_{ts}.png"
        filepath = session_dir / filename
        img.save(filepath, format="PNG", optimize=True)
        saved.append(filepath)

    return saved


# ── Logging ──────────────────────────────────────────────────────────────────
def make_session_dir(base_dir: str, session_name: str) -> Path:
    ts        = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in session_name)
    folder    = Path(base_dir) / f"{safe_name}_{ts}"
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def write_log(session_dir: Path, line: str):
    with open(session_dir / "capture_log.txt", "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="HR Exam Screen Capture Tool")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL_SECONDS)
    parser.add_argument("--output",   type=str,   default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--session",  type=str,   default="ExamSession")
    args = parser.parse_args()

    session_dir = make_session_dir(args.output, args.session)
    print(f"[INFO] Session folder  : {session_dir.resolve()}")
    print(f"[INFO] Capture interval: {args.interval}s")
    print(f"[INFO] Grab timeout    : {GRAB_TIMEOUT_SECONDS}s")
    print(f"[INFO] Press Ctrl+C to stop.\n")

    start_time = datetime.now()
    write_log(session_dir, f"Session started : {start_time.isoformat()}")
    write_log(session_dir, f"Interval        : {args.interval}s")
    write_log(session_dir, "-" * 60)

    cameras       = build_cameras()
    capture_index = 0

    try:
        while _running:
            loop_start = time.monotonic()

            try:
                paths = capture_all_monitors(cameras, session_dir, capture_index)
                ts_label = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                for p in paths:
                    log_line = f"[{ts_label}] #{capture_index:05d} -> {p.name}"
                    print(log_line)
                    write_log(session_dir, log_line)
                capture_index += 1
            except Exception as exc:
                err = f"[ERROR] Capture #{capture_index} failed: {exc}"
                print(err)
                write_log(session_dir, err)

            elapsed = time.monotonic() - loop_start
            time.sleep(max(0.0, args.interval - elapsed))

    finally:
        for entry in cameras:
            _release_camera(entry.get("cam"))

    end_time = datetime.now()
    summary  = (
        f"\nSession ended   : {end_time.isoformat()}\n"
        f"Total captures  : {capture_index}\n"
        f"Duration        : {end_time - start_time}\n"
        f"Files saved in  : {session_dir.resolve()}"
    )
    print(summary)
    write_log(session_dir, summary)


if __name__ == "__main__":
    main()
