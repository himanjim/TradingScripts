#!/usr/bin/env python3
"""
realtime_incoming_audio_transcriber_v3_clipboard.py

Low-latency transcription of Windows speaker/output audio.

Key reliability changes
-----------------------
1. WASAPI loopback capture runs in its own thread.
2. Vosk recognition runs separately, so recognition cannot block recording.
3. The recorder uses a larger internal buffer while still delivering small
   low-latency audio blocks.
4. SoundCard's repetitive "data discontinuity" warning is shown at most once.
5. Each final recognised utterance is copied to the Windows clipboard.

Install:
    py -m pip install vosk soundcard numpy

Run:
    py realtime_incoming_audio_transcriber_v3_clipboard.py

Examples:
    py realtime_incoming_audio_transcriber_v3_clipboard.py --list-devices
    py realtime_incoming_audio_transcriber_v3_clipboard.py --speaker "Headphones"
    py realtime_incoming_audio_transcriber_v3_clipboard.py --lang en-in
    py realtime_incoming_audio_transcriber_v3_clipboard.py --block-ms 100 --buffer-ms 600

Stop:
    Ctrl+C
"""

from __future__ import annotations

import argparse
import json
import queue
import re
import subprocess
import sys
import threading
import warnings
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import soundcard as sc
from vosk import KaldiRecognizer, Model, SetLogLevel


DEFAULT_RATE = 16_000
DEFAULT_BLOCK_MS = 100
DEFAULT_BUFFER_MS = 600
DEFAULT_OUTPUT = "incoming_audio_realtime.txt"
QUEUE_CAPACITY = 200


# SoundCard may emit this repeatedly after a transient WASAPI gap.
# Keep one occurrence visible, but prevent console flooding.
warnings.filterwarnings(
    "once",
    message=r"data discontinuity in recording",
    module=r"soundcard\.mediafoundation",
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream Windows speaker audio to a text file using Vosk."
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--speaker",
        default=None,
        help="Output device name/partial name. Uses Windows default if omitted.",
    )
    parser.add_argument("--list-devices", action="store_true")
    parser.add_argument(
        "--lang",
        default="en-us",
        help="Vosk model language, such as en-us or en-in.",
    )
    parser.add_argument(
        "--model-path",
        default=None,
        help="Optional path to an already downloaded Vosk model.",
    )
    parser.add_argument(
        "--sample-rate",
        type=int,
        default=DEFAULT_RATE,
        help="Capture and recognizer sample rate. Default: 16000.",
    )
    parser.add_argument(
        "--block-ms",
        type=int,
        default=DEFAULT_BLOCK_MS,
        help="Delivered audio block duration. Default: 100 ms.",
    )
    parser.add_argument(
        "--buffer-ms",
        type=int,
        default=DEFAULT_BUFFER_MS,
        help=(
            "Internal WASAPI recorder buffer. This improves continuity without "
            "delaying the 100 ms blocks. Default: 600 ms."
        ),
    )
    parser.add_argument(
        "--hold-back-words",
        type=int,
        default=1,
        help="Trailing unstable words retained before appending. Default: 1.",
    )
    parser.add_argument(
        "--final-only",
        action="store_true",
        help="Write to file only when the speaker pauses.",
    )
    parser.add_argument(
        "--no-clipboard",
        action="store_true",
        help="Disable automatic copying of final recognised text to clipboard.",
    )
    parser.add_argument("--show-vosk-logs", action="store_true")
    return parser.parse_args()


def list_devices() -> None:
    default = sc.default_speaker()
    print("Available output devices:\n")
    for number, speaker in enumerate(sc.all_speakers(), 1):
        marker = " [DEFAULT]" if default and speaker.id == default.id else ""
        print(f"{number:>2}. {speaker.name}{marker}")
        print(f"    ID: {speaker.id}")


def choose_speaker(query: Optional[str]):
    if not query:
        speaker = sc.default_speaker()
        if speaker is None:
            raise RuntimeError("No default Windows output device was found.")
        return speaker

    try:
        return sc.get_speaker(query)
    except Exception as exc:
        available = "\n".join(
            f"  - {speaker.name}" for speaker in sc.all_speakers()
        )
        raise RuntimeError(
            f"Could not find an output device matching {query!r}.\n"
            f"Available devices:\n{available}"
        ) from exc


def all_microphones_compat():
    try:
        return sc.all_microphones(include_loopback=True)
    except TypeError:
        return sc.all_microphones(True)


def get_microphone_compat(device_id):
    try:
        return sc.get_microphone(device_id, include_loopback=True)
    except TypeError:
        return sc.get_microphone(device_id, True)


def choose_loopback(speaker):
    try:
        microphone = get_microphone_compat(speaker.id)
        if microphone and getattr(microphone, "isloopback", False):
            return microphone
    except Exception:
        pass

    loopbacks = [
        microphone
        for microphone in all_microphones_compat()
        if getattr(microphone, "isloopback", False)
    ]

    wanted = speaker.name.casefold()
    for microphone in loopbacks:
        candidate = microphone.name.casefold()
        if wanted in candidate or candidate in wanted:
            return microphone

    if len(loopbacks) == 1:
        return loopbacks[0]

    available = "\n".join(
        f"  - {microphone.name}" for microphone in loopbacks
    ) or "  none"

    raise RuntimeError(
        f"No loopback endpoint matched {speaker.name!r}.\n"
        f"Available loopbacks:\n{available}"
    )


def float_audio_to_pcm16(audio: np.ndarray) -> bytes:
    data = np.asarray(audio, dtype=np.float32)

    if data.ndim == 2:
        data = data.mean(axis=1, dtype=np.float32)
    elif data.ndim != 1:
        raise ValueError(f"Unexpected audio shape: {data.shape}")

    data = np.nan_to_num(data, nan=0.0, posinf=1.0, neginf=-1.0)
    data = np.clip(data, -1.0, 1.0)
    return (data * 32767.0).astype("<i2", copy=False).tobytes()


def extract_text(payload: str, key: str) -> str:
    try:
        value = json.loads(payload).get(key, "")
    except json.JSONDecodeError:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def common_prefix_length(first: list[str], second: list[str]) -> int:
    count = 0
    for left, right in zip(first, second):
        if left.casefold() != right.casefold():
            break
        count += 1
    return count


def copy_to_windows_clipboard(text: str) -> bool:
    """
    Copy Unicode text to the Windows clipboard using the built-in clip.exe.

    Returns True on success. No third-party clipboard package is required.
    """
    text = text.strip()
    if not text:
        return False

    try:
        subprocess.run(
            ["clip.exe"],
            input=text,
            text=True,
            encoding="utf-16le",
            check=True,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        return True
    except Exception as exc:
        print(f"\nClipboard warning: {exc}", file=sys.stderr)
        return False


class TranscriptWriter:
    def __init__(
        self,
        path: Path,
        hold_back_words: int,
        append_partials: bool,
        copy_final_to_clipboard: bool,
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.file = path.open("a", encoding="utf-8", buffering=1, newline="\n")
        self.hold_back = max(0, hold_back_words)
        self.append_partials = append_partials
        self.copy_final_to_clipboard = copy_final_to_clipboard
        self.previous_partial: list[str] = []
        self.committed = 0
        self.line_started = False
        self.console_width = 0

    def start_session(self, speaker_name: str, model_name: str) -> None:
        self.file.write(
            f"\n--- Session started {datetime.now():%Y-%m-%d %H:%M:%S} | "
            f"Output: {speaker_name} | Model: {model_name} ---\n"
        )
        self.file.flush()

    def _start_line(self) -> None:
        if not self.line_started:
            self.file.write(f"[{datetime.now():%H:%M:%S}] ")
            self.file.flush()
            self.line_started = True

    def _append_words(self, words: list[str]) -> None:
        if not words:
            return

        self._start_line()
        separator = "" if self.committed == 0 else " "
        self.file.write(separator + " ".join(words))
        self.file.flush()

    def accept_partial(self, text: str) -> None:
        if not text:
            return

        display = f"Listening: {text}"
        padding = " " * max(0, self.console_width - len(display))
        print("\r" + display + padding, end="", flush=True)
        self.console_width = len(display)

        current = text.split()

        if self.append_partials and self.previous_partial:
            stable_end = max(
                0,
                common_prefix_length(self.previous_partial, current)
                - self.hold_back,
            )

            if stable_end > self.committed:
                self._append_words(current[self.committed:stable_end])
                self.committed = stable_end

        self.previous_partial = current

    def accept_final(self, text: str) -> None:
        if self.console_width:
            print(
                "\r" + (" " * self.console_width) + "\r",
                end="",
                flush=True,
            )
            self.console_width = 0

        words = text.split()
        if words:
            self._append_words(words[self.committed:])
            self.file.write("\n")
            self.file.flush()

            copied = False
            if self.copy_final_to_clipboard:
                copied = copy_to_windows_clipboard(text)

            clipboard_note = " [copied to clipboard]" if copied else ""
            print(f"Final: {text}{clipboard_note}", flush=True)

        self.previous_partial = []
        self.committed = 0
        self.line_started = False

    def close(self) -> None:
        if self.line_started:
            self.file.write("\n")
        self.file.write(
            f"--- Session ended {datetime.now():%Y-%m-%d %H:%M:%S} ---\n"
        )
        self.file.flush()
        self.file.close()


def load_model(args: argparse.Namespace) -> tuple[Model, str]:
    if args.model_path:
        path = Path(args.model_path).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"Model path does not exist: {path}")
        return Model(str(path)), str(path)

    return Model(lang=args.lang), args.lang


def capture_audio(
    loopback,
    sample_rate: int,
    frames_per_block: int,
    recorder_buffer_frames: int,
    audio_queue: "queue.Queue[Optional[bytes]]",
    stop_event: threading.Event,
) -> None:
    """
    Continuously capture audio independently of speech recognition.
    """
    try:
        with loopback.recorder(
            samplerate=sample_rate,
            channels=None,
            blocksize=recorder_buffer_frames,
        ) as recorder:
            while not stop_event.is_set():
                audio = recorder.record(numframes=frames_per_block)
                pcm = float_audio_to_pcm16(audio)

                try:
                    audio_queue.put(pcm, timeout=0.2)
                except queue.Full:
                    # Prefer current audio over stale audio if recognition
                    # somehow falls far behind.
                    try:
                        audio_queue.get_nowait()
                    except queue.Empty:
                        pass
                    try:
                        audio_queue.put_nowait(pcm)
                    except queue.Full:
                        pass

    except Exception as exc:
        print(f"\nAudio capture error: {exc}", file=sys.stderr)
        stop_event.set()
    finally:
        try:
            audio_queue.put_nowait(None)
        except queue.Full:
            pass


def main() -> int:
    args = parse_arguments()

    if args.list_devices:
        list_devices()
        return 0

    if not 20 <= args.block_ms <= 1000:
        print("--block-ms must be between 20 and 1000.", file=sys.stderr)
        return 2

    if args.buffer_ms < args.block_ms:
        print("--buffer-ms must be at least --block-ms.", file=sys.stderr)
        return 2

    if args.sample_rate <= 0:
        print("--sample-rate must be positive.", file=sys.stderr)
        return 2

    if args.hold_back_words < 0:
        print("--hold-back-words cannot be negative.", file=sys.stderr)
        return 2

    SetLogLevel(0 if args.show_vosk_logs else -1)

    try:
        speaker = choose_speaker(args.speaker)
        loopback = choose_loopback(speaker)

        print("Loading streaming model...")
        model, model_name = load_model(args)
    except Exception as exc:
        print(f"Setup error: {exc}", file=sys.stderr)
        return 1

    recognizer = KaldiRecognizer(model, args.sample_rate)
    recognizer.SetWords(True)
    recognizer.SetPartialWords(True)

    output_path = Path(args.output).expanduser().resolve()
    writer = TranscriptWriter(
        output_path,
        hold_back_words=args.hold_back_words,
        append_partials=not args.final_only,
        copy_final_to_clipboard=not args.no_clipboard,
    )
    writer.start_session(speaker.name, model_name)

    frames_per_block = max(
        1,
        int(args.sample_rate * args.block_ms / 1000),
    )
    recorder_buffer_frames = max(
        frames_per_block,
        int(args.sample_rate * args.buffer_ms / 1000),
    )

    audio_queue: "queue.Queue[Optional[bytes]]" = queue.Queue(
        maxsize=QUEUE_CAPACITY
    )
    stop_event = threading.Event()

    capture_thread = threading.Thread(
        target=capture_audio,
        args=(
            loopback,
            args.sample_rate,
            frames_per_block,
            recorder_buffer_frames,
            audio_queue,
            stop_event,
        ),
        name="WASAPI-loopback-capture",
        daemon=True,
    )

    print("\nReal-time transcription active.")
    print(f"Output device : {speaker.name}")
    print(f"Loopback input: {loopback.name}")
    print(f"Audio delivery: {args.block_ms} ms")
    print(f"WASAPI buffer : {args.buffer_ms} ms")
    print(f"Text file     : {output_path}")
    print(
        "Clipboard     : "
        + ("disabled" if args.no_clipboard else "latest final utterance")
    )
    print("Press Ctrl+C to stop.\n")

    capture_thread.start()
    return_code = 0

    try:
        while True:
            try:
                pcm = audio_queue.get(timeout=0.5)
            except queue.Empty:
                if stop_event.is_set():
                    break
                continue

            if pcm is None:
                break

            if recognizer.AcceptWaveform(pcm):
                writer.accept_final(
                    extract_text(recognizer.Result(), "text")
                )
            else:
                writer.accept_partial(
                    extract_text(recognizer.PartialResult(), "partial")
                )

    except KeyboardInterrupt:
        print("\nStopping...")
        stop_event.set()

    except Exception as exc:
        print(f"\nRecognition error: {exc}", file=sys.stderr)
        stop_event.set()
        return_code = 1

    finally:
        stop_event.set()
        capture_thread.join(timeout=2.0)

        try:
            writer.accept_final(
                extract_text(recognizer.FinalResult(), "text")
            )
        except Exception:
            pass

        writer.close()

    print(f"Transcript saved to:\n{output_path}")
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
