#!/usr/bin/env python3
"""
Near-real-time Windows output-audio transcription.

Captures the selected speaker/output through WASAPI loopback, not the normal
microphone. Vosk processes ~100 ms blocks and appends stable words while speech
is still arriving.

Install:
    py -m pip install vosk soundcard numpy

Run:
    py realtime_incoming_audio_transcriber_fixed.py
    py realtime_incoming_audio_transcriber_fixed.py --list-devices
    py realtime_incoming_audio_transcriber_fixed.py --speaker "Headphones"
    py realtime_incoming_audio_transcriber_fixed.py --lang en-in

Stop with Ctrl+C.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import soundcard as sc
from vosk import KaldiRecognizer, Model, SetLogLevel


DEFAULT_RATE = 16_000
DEFAULT_BLOCK_MS = 100
DEFAULT_OUTPUT = "incoming_audio_realtime.txt"


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream Windows speaker audio to a text file using Vosk."
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--speaker",
        default=None,
        help="Output device name/partial name. Default Windows output if omitted.",
    )
    parser.add_argument("--list-devices", action="store_true")
    parser.add_argument(
        "--lang",
        default="en-us",
        help="Vosk language model identifier, e.g. en-us or en-in.",
    )
    parser.add_argument(
        "--model-path",
        default=None,
        help="Optional path to a downloaded Vosk model.",
    )
    parser.add_argument("--sample-rate", type=int, default=DEFAULT_RATE)
    parser.add_argument(
        "--block-ms",
        type=int,
        default=DEFAULT_BLOCK_MS,
        help="Audio block size. 50-150 ms is practical; default 100.",
    )
    parser.add_argument(
        "--hold-back-words",
        type=int,
        default=1,
        help="Trailing unstable words kept in memory; default 1.",
    )
    parser.add_argument(
        "--final-only",
        action="store_true",
        help="Write to file only when the speaker pauses.",
    )
    parser.add_argument("--show-vosk-logs", action="store_true")
    return parser.parse_args()


def list_devices() -> None:
    default = sc.default_speaker()
    for number, speaker in enumerate(sc.all_speakers(), 1):
        marker = " [DEFAULT]" if default and speaker.id == default.id else ""
        print(f"{number:>2}. {speaker.name}{marker}\n    ID: {speaker.id}")


def choose_speaker(query: Optional[str]):
    if not query:
        speaker = sc.default_speaker()
        if speaker is None:
            raise RuntimeError("No default Windows output device was found.")
        return speaker
    return sc.get_speaker(query)


def _get_microphone_compat(device_id):
    """
    Return a loopback microphone across old and new SoundCard releases.

    Some SoundCard versions accept only ``include_loopback`` and raise:
        unexpected keyword argument 'exclude_monitors'
    """
    try:
        return sc.get_microphone(device_id, include_loopback=True)
    except TypeError:
        # Compatibility with releases where include_loopback is positional.
        return sc.get_microphone(device_id, True)


def _all_microphones_compat():
    """List microphones including loopbacks across SoundCard versions."""
    try:
        return sc.all_microphones(include_loopback=True)
    except TypeError:
        # Compatibility with releases where include_loopback is positional.
        return sc.all_microphones(True)


def choose_loopback(speaker):
    try:
        microphone = _get_microphone_compat(speaker.id)
        if microphone and getattr(microphone, "isloopback", False):
            return microphone
    except Exception:
        pass

    loopbacks = [
        mic
        for mic in _all_microphones_compat()
        if getattr(mic, "isloopback", False)
    ]

    wanted = speaker.name.casefold()
    for microphone in loopbacks:
        name = microphone.name.casefold()
        if wanted in name or name in wanted:
            return microphone

    if len(loopbacks) == 1:
        return loopbacks[0]

    available = "\n".join(f"  - {mic.name}" for mic in loopbacks) or "  none"
    raise RuntimeError(
        f"No loopback endpoint matched {speaker.name!r}.\n"
        f"Available loopbacks:\n{available}"
    )


def pcm16(audio: np.ndarray) -> bytes:
    data = np.asarray(audio, dtype=np.float32)
    if data.ndim == 2:
        data = data.mean(axis=1, dtype=np.float32)
    elif data.ndim != 1:
        raise ValueError(f"Unexpected audio shape: {data.shape}")

    data = np.nan_to_num(data, nan=0.0, posinf=1.0, neginf=-1.0)
    data = np.clip(data, -1.0, 1.0)
    return (data * 32767.0).astype("<i2", copy=False).tobytes()


def result_text(payload: str, key: str) -> str:
    try:
        value = json.loads(payload).get(key, "")
    except json.JSONDecodeError:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def common_prefix(a: list[str], b: list[str]) -> int:
    count = 0
    for first, second in zip(a, b):
        if first.casefold() != second.casefold():
            break
        count += 1
    return count


class TranscriptWriter:
    """Append confirmed partial words and then finish each utterance."""

    def __init__(
        self,
        path: Path,
        hold_back_words: int,
        append_partials: bool,
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.file = path.open("a", encoding="utf-8", buffering=1, newline="\n")
        self.hold_back = max(0, hold_back_words)
        self.append_partials = append_partials
        self.previous_partial: list[str] = []
        self.committed = 0
        self.line_started = False
        self.console_width = 0

    def session_start(self, speaker: str, model: str) -> None:
        self.file.write(
            f"\n--- Session started {datetime.now():%Y-%m-%d %H:%M:%S} | "
            f"Output: {speaker} | Model: {model} ---\n"
        )

    def _start_line(self) -> None:
        if not self.line_started:
            self.file.write(f"[{datetime.now():%H:%M:%S}] ")
            self.line_started = True

    def _append(self, words: list[str]) -> None:
        if not words:
            return
        self._start_line()
        separator = "" if self.committed == 0 else " "
        self.file.write(separator + " ".join(words))
        self.file.flush()

    def partial(self, text: str) -> None:
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
                common_prefix(self.previous_partial, current) - self.hold_back,
            )
            if stable_end > self.committed:
                self._append(current[self.committed:stable_end])
                self.committed = stable_end
        self.previous_partial = current

    def final(self, text: str) -> None:
        if self.console_width:
            print("\r" + " " * self.console_width + "\r", end="", flush=True)
            self.console_width = 0

        words = text.split()
        if words:
            self._append(words[self.committed:])
            self.file.write("\n")
            self.file.flush()
            print(f"Final: {text}")

        self.previous_partial = []
        self.committed = 0
        self.line_started = False

    def close(self) -> None:
        if self.line_started:
            self.file.write("\n")
        self.file.write(f"--- Session ended {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
        self.file.close()


def load_model(args: argparse.Namespace) -> tuple[Model, str]:
    if args.model_path:
        path = Path(args.model_path).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"Model path does not exist: {path}")
        return Model(str(path)), str(path)
    return Model(lang=args.lang), args.lang


def main() -> int:
    args = arguments()

    if args.list_devices:
        list_devices()
        return 0
    if not 20 <= args.block_ms <= 1000:
        print("--block-ms must be between 20 and 1000.", file=sys.stderr)
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

    output = Path(args.output).expanduser().resolve()
    writer = TranscriptWriter(
        output,
        hold_back_words=args.hold_back_words,
        append_partials=not args.final_only,
    )
    writer.session_start(speaker.name, model_name)

    frames = max(1, int(args.sample_rate * args.block_ms / 1000))

    print("\nReal-time transcription active.")
    print(f"Output device : {speaker.name}")
    print(f"Loopback input: {loopback.name}")
    print(f"Audio blocks  : {args.block_ms} ms")
    print(f"Text file     : {output}")
    print("Press Ctrl+C to stop.\n")

    return_code = 0
    try:
        with loopback.recorder(
            samplerate=args.sample_rate,
            channels=None,
            blocksize=frames,
        ) as recorder:
            while True:
                data = pcm16(recorder.record(numframes=frames))
                if recognizer.AcceptWaveform(data):
                    writer.final(result_text(recognizer.Result(), "text"))
                else:
                    writer.partial(result_text(recognizer.PartialResult(), "partial"))
    except KeyboardInterrupt:
        writer.final(result_text(recognizer.FinalResult(), "text"))
        print("\nStopped.")
    except Exception as exc:
        print(f"\nRuntime error: {exc}", file=sys.stderr)
        return_code = 1
    finally:
        writer.close()

    print(f"Transcript saved to:\n{output}")
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
