#!/usr/bin/env python3
"""Runs the official 1M dev correctness test on Windows.

Downloads the official generator and golden CSV answer, builds the generator with
Rust/Cargo, runs this repo's current pipeline, and compares exact rows.
"""
from __future__ import annotations
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / 'dev_test_downloads'
BASE = 'https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/'


def step(message):
    print('\n' + '=' * 65 + '\n' + message + '\n' + '=' * 65, flush=True)


def fetch(name):
    CACHE.mkdir(exist_ok=True)
    target = CACHE / name
    if not target.is_file() or target.stat().st_size < 100:
        print('Downloading official ' + name + ' ...', flush=True)
        request = urllib.request.Request(BASE + name, headers={'User-Agent': 'Mozilla/5.0 ShoppingSessionsDevTester/1.0'})
        try:
            with urllib.request.urlopen(request, timeout=120) as response, open(target, 'wb') as out:
                shutil.copyfileobj(response, out)
        except Exception:
            target.unlink(missing_ok=True)
            raise
    if not zipfile.is_zipfile(target):
        raise ValueError(f'Download is not a ZIP file: {target}')
    return target


def unpack(src, dst):
    if not dst.is_dir():
        dst.mkdir(parents=True)
        with zipfile.ZipFile(src) as archive:
            archive.extractall(dst)


def generator_manifest():
    manifests = list((CACHE / 'generator').rglob('Cargo.toml'))
    if not manifests:
        raise FileNotFoundError('No Cargo.toml in official generator.zip; please send me the contents or screenshot.')
    # Try to pick the workspace root by shortest path.
    return min(manifests, key=lambda p: len(p.parts))


def cargo_binary(manifest):
    p = subprocess.run(['cargo', 'metadata', '--no-deps', '--format-version', '1',
                        '--manifest-path', str(manifest)], check=True, text=True,
                       stdout=subprocess.PIPE)
    meta = json.loads(p.stdout)
    bins = [(pkg['id'], t['name']) for pkg in meta['packages'] for t in pkg['targets'] if 'bin' in t['kind']]
    if not bins:
        raise ValueError('Rust generator has no executable target.')
    for _, name in bins:
        if name == 'generate':
            return name
    if len(bins) == 1:
        return bins[0][1]
    raise ValueError('Generator has several binary targets. Please send a screenshot: ' + repr(bins))


def run():
    step('1 / 4: Checking Python and Rust')
    print('Python: ' + sys.version.split()[0])
    try:
        import duckdb  # noqa: F401
        import polars  # noqa: F401
    except ImportError:
        print('Missing Python packages. Run TEST_ON_WINDOWS.bat first.')
        return 2
    if shutil.which('cargo') is None:
        print('Rust is needed to compile the official data generator.\n'
              'Install it from https://rustup.rs/ (default installation),\n'
              'CLOSE this terminal, then double-click TEST_1M_ON_WINDOWS.bat again.\n'
              'Windows may also prompt you to install Visual Studio C++ Build Tools.')
        return 2

    step('2 / 4: Downloading the official generator and reference answer')
    generator_zip = fetch('generator.zip')
    answer_zip = fetch('dev-answer.zip')
    unpack(generator_zip, CACHE / 'generator')
    unpack(answer_zip, CACHE / 'dev-answer')
    manifest = generator_manifest()
    target = generator_manifest().parent
    print('Generator source: ' + str(target))

    step('3 / 4: Generating the official 1,000,000-event dev set (seed=1)')
    dev = ROOT / 'dev'
    if not (dev / 'manifest.json').is_file():
        bin_name = cargo_binary(manifest)
        command = ['cargo', 'run', '--release', '--manifest-path', str(manifest),
                   '--bin', bin_name, '--', '--seed', '1', '--events', '1000000',
                   '--clicks', '250000', '--output', str(dev)]
        subprocess.run(command, check=True, cwd=target)
    else:
        print('Found existing dev dataset; reusing it.')

    output = ROOT / 'dev_out'
    scratch = ROOT / 'dev_scratch'
    shutil.rmtree(output, ignore_errors=True)
    shutil.rmtree(scratch, ignore_errors=True)
    output.mkdir(parents=True, exist_ok=True)
    scratch.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update(DATA_DIR=str(dev), OUTPUT_DIR=str(output), SCRATCH_DIR=str(scratch),
               NCPU=str(min(4, os.cpu_count() or 2)), MEM_GB='4')
    print('Running our pipeline...')
    t0 = time.perf_counter()
    subprocess.run([sys.executable, str(ROOT / 'pipeline' / 'pipeline.py')],
                   env=env, cwd=ROOT, check=True)
    duration = time.perf_counter() - t0
    print(f'OUR PIPELINE TIME ON THIS WINDOWS LAPTOP: {duration:.2f} seconds')
    print('This is NOT a valid leaderboard time: different CPU, OS, data size.')

    step('4 / 4: Comparing against the official answer, row for row')
    answers = list((CACHE / 'dev-answer').rglob('user_daily.csv'))
    folders = [p.parent for p in answers if (p.parent / 'campaign_daily.csv').is_file()]
    if len(folders) != 1:
        raise FileNotFoundError('Could not locate exactly one expected user_daily.csv and campaign_daily.csv pair.')
    result = subprocess.run([sys.executable, str(ROOT / 'scripts' / 'compare.py'),
                             str(output), str(folders[0])], cwd=ROOT)
    if result.returncode == 0:
        print('\nSUCCESS: official 1M dev dataset matched exactly!')
    else:
        print('\nFAILED: output differences detected; send me the terminal screenshot.')
    return result.returncode


if __name__ == '__main__':
    try:
        sys.exit(run())
    except Exception as e:
        print('\nERROR: ' + str(e), file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
