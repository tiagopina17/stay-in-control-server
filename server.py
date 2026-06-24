"""
Bridges Pulsoid → processes calm score → serves Unity via HTTP polling
"""

import asyncio
import json
import os
import time
import threading
from contextlib import asynccontextmanager
import websockets
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import uvicorn

# CSV logging (optional — works locally, silently skipped if filesystem is read-only)
try:
    import csv
    _CSV_AVAILABLE = True
except ImportError:
    _CSV_AVAILABLE = False

# ─────────────────────────────────────────────
# CONFIG — set PULSOID_TOKEN as an environment variable
# ─────────────────────────────────────────────

PULSOID_TOKEN = os.environ.get("PULSOID_TOKEN", "")
if not PULSOID_TOKEN:
    print("[Config] WARNING: PULSOID_TOKEN env var is not set. Pulsoid will not connect.")

PULSOID_WS_URL = f"wss://dev.pulsoid.net/api/v1/data/real_time?access_token={PULSOID_TOKEN}"
HTTP_PORT = int(os.environ.get("PORT", 8000))  # Railway injects PORT automatically

# ─────────────────────────────────────────────
# SHARED STATE
# ─────────────────────────────────────────────

state = {
    "heart_rate": 0,
    "measured_at": 0,
    "baseline": 70,            # BPM — set via dashboard or overridden per player
    "calm_threshold": 0.80,    # calm_score must be >= this for IsCalm = true
    "pulsoid_connected": False,
    "last_update": 0,
    # ── Dev overrides ───────────────────────────────────────────────────────
    "force_skip": False,       # POST /force-skip/on — bypasses calm check entirely
    "simulate_mode": False,    # POST /simulate/on — fakes HR with a sine wave
    "simulate_hr": 70,         # current simulated HR (updated by background task)
    "simulate_amplitude": 10,  # BPM swing above/below baseline in simulate mode
    "simulate_pinned": False,  # True when HR is manually pinned via /simulate/set-hr
}

LOG_FILE = os.environ.get("LOG_FILE", "biofeedback_log.csv")
_log_enabled = True  # flipped to False if writing fails (e.g. read-only cloud FS)

current_level = {
    "track": "None",
    "level_index": -1,
    "level_name": ""
}


def effective_heart_rate() -> int:
    """Returns the HR Unity and the calm calculation should act on."""
    if state["simulate_mode"]:
        return state["simulate_hr"]
    return state["heart_rate"]


MAX_STRESS_MULTIPLIER = 1.571  # HR at 1.4× baseline = score 0.0 (max stress)

def get_calm_score() -> float:
    """
    Normalized calm score in [0.0, 1.0], relative to the player's baseline.
      baseline HR         → 1.0 (perfectly calm)
      baseline × 1.2     → 0.5 (half stressed)
      baseline × 1.4     → 0.0 (max stress, clamped)
    """
    hr = effective_heart_rate()
    if hr <= 0:
        return 0.0
    baseline = state["baseline"]
    stress = (hr - baseline) / (baseline * (MAX_STRESS_MULTIPLIER - 1))
    stress = max(0.0, min(1.0, stress))
    return round(1.0 - stress, 3)


def is_calm() -> bool:
    if state["force_skip"]:
        return True
    if state["simulate_mode"]:
        return get_calm_score() >= state["calm_threshold"]
    # real mode: also check data isn't stale (>20s old)
    data_age = time.time() - state["last_update"]
    if data_age > 20 or state["heart_rate"] <= 0:
        return False
    return get_calm_score() >= state["calm_threshold"]


def log_data():
    global _log_enabled
    if not _log_enabled or not _CSV_AVAILABLE:
        return
    try:
        with open(LOG_FILE, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                time.time(),
                current_level["track"],
                current_level["level_index"],
                current_level["level_name"],
                effective_heart_rate(),
                state["baseline"],
                get_calm_score(),
                is_calm()
            ])
    except OSError as e:
        print(f"[Log] Cannot write to {LOG_FILE}: {e}. Logging disabled.")
        _log_enabled = False


# ─────────────────────────────────────────────
# PULSOID WEBSOCKET LISTENER (background thread)
# ─────────────────────────────────────────────

async def pulsoid_listener():
    retry_delay = 3
    while True:
        try:
            print(f"[Pulsoid] Connecting to WebSocket...")
            async with websockets.connect(PULSOID_WS_URL) as ws:
                state["pulsoid_connected"] = True
                retry_delay = 3
                print("[Pulsoid] Connected. Listening for heart rate...")
                async for message in ws:
                    try:
                        data = json.loads(message)
                        hr = data.get("data", {}).get("heart_rate", 0)
                        measured_at = data.get("measured_at", 0)
                        if hr > 0:
                            state["heart_rate"] = hr
                            state["measured_at"] = measured_at
                            state["last_update"] = time.time()
                            log_data()
                    except json.JSONDecodeError:
                        pass
        except Exception as e:
            state["pulsoid_connected"] = False
            print(f"[Pulsoid] Disconnected: {e}. Retrying in {retry_delay}s...")
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 30)


async def simulate_hr_task():
    """Updates state['simulate_hr'] with a sine wave drifting around baseline."""
    import math
    t = 0.0
    while True:
        if state["simulate_mode"] and not state["simulate_pinned"]:
            amp = state["simulate_amplitude"]
            baseline = state["baseline"]
            # period ~20 s — slow enough to watch the calm bar move
            state["simulate_hr"] = max(30, round(baseline + amp * math.sin(t * 0.314)))
            state["last_update"] = time.time()
            t += 1.0
        await asyncio.sleep(1.0)


def start_simulate_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(simulate_hr_task())


def start_pulsoid_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(pulsoid_listener())


# ─────────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    t = threading.Thread(target=start_pulsoid_thread, daemon=True)
    t.start()
    s = threading.Thread(target=start_simulate_thread, daemon=True)
    s.start()
    print("[Server] Pulsoid listener and simulate threads started.")
    yield


app = FastAPI(title="Biofeedback Server", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────
# UNITY ENDPOINT — poll this every 1-2 seconds
# ─────────────────────────────────────────────

@app.get("/status")
def get_status():
    data_age = round(time.time() - state["last_update"], 1) if state["last_update"] > 0 else -1
    hr = effective_heart_rate()
    return {
        "heart_rate": hr,
        "baseline": state["baseline"],
        "calm_score": get_calm_score(),
        "calm_threshold": state["calm_threshold"],
        "is_calm": is_calm(),
        "pulsoid_connected": state["pulsoid_connected"],
        "data_age_seconds": data_age,
        "force_skip": state["force_skip"],
        "simulate_mode": state["simulate_mode"],
    }


# ─────────────────────────────────────────────
# DASHBOARD ENDPOINTS
# ─────────────────────────────────────────────

class BaselineUpdate(BaseModel):
    baseline: int

class ThresholdUpdate(BaseModel):
    threshold: float


@app.post("/set-baseline")
def set_baseline(body: BaselineUpdate):
    if body.baseline < 30 or body.baseline > 250:
        raise HTTPException(status_code=400, detail="Baseline must be between 30 and 250 BPM")
    state["baseline"] = body.baseline
    return {"ok": True, "baseline": state["baseline"]}


@app.post("/set-threshold")
def set_threshold(body: ThresholdUpdate):
    if body.threshold < 0.5 or body.threshold > 1.5:
        raise HTTPException(status_code=400, detail="Threshold must be between 0.5 and 1.5")
    state["calm_threshold"] = round(body.threshold, 2)
    return {"ok": True, "calm_threshold": state["calm_threshold"]}


@app.post("/capture-baseline")
def capture_baseline():
    """Capture current HR as the player's baseline."""
    hr = state["heart_rate"]
    if hr <= 0:
        raise HTTPException(status_code=412, detail="No heart rate data available yet")
    state["baseline"] = hr
    return {"ok": True, "baseline_captured": hr}


# ─────────────────────────────────────────────
# DEV OVERRIDE ENDPOINTS
# ─────────────────────────────────────────────

@app.post("/force-skip/on")
def force_skip_on():
    """Force is_calm = True regardless of HR. Useful for level skipping during dev."""
    state["force_skip"] = True
    return {"ok": True, "force_skip": True}


@app.post("/force-skip/off")
def force_skip_off():
    state["force_skip"] = False
    return {"ok": True, "force_skip": False}


class SimulateConfig(BaseModel):
    amplitude: int = 10   # BPM swing around baseline


@app.post("/simulate/on")
def simulate_on(body: SimulateConfig = SimulateConfig()):
    if body.amplitude < 0 or body.amplitude > 60:
        raise HTTPException(status_code=400, detail="Amplitude must be 0–60 BPM")
    state["simulate_mode"] = True
    state["simulate_pinned"] = False  # resume sine wave
    state["simulate_amplitude"] = body.amplitude
    return {"ok": True, "simulate_mode": True, "amplitude": body.amplitude}


@app.post("/simulate/off")
def simulate_off():
    state["simulate_mode"] = False
    state["simulate_pinned"] = False
    return {"ok": True, "simulate_mode": False}


class SimulateHROverride(BaseModel):
    heart_rate: int


@app.post("/simulate/set-hr")
def simulate_set_hr(body: SimulateHROverride):
    """Pin simulated HR to an exact value (pauses sine wave until simulate/on is called)."""
    if body.heart_rate < 30 or body.heart_rate > 250:
        raise HTTPException(status_code=400, detail="HR must be 30–250 BPM")
    state["simulate_mode"] = True
    state["simulate_pinned"] = True
    state["simulate_hr"] = body.heart_rate
    state["last_update"] = time.time()
    return {"ok": True, "simulate_hr": body.heart_rate}


class LevelUpdate(BaseModel):
    track: str
    level_index: int
    level_name: str = ""

@app.post("/set-level")
def set_level(body: LevelUpdate):
    current_level["track"] = body.track
    current_level["level_index"] = body.level_index
    current_level["level_name"] = body.level_name
    return {"ok": True}


# ─────────────────────────────────────────────
# DASHBOARD HTML
# ─────────────────────────────────────────────

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Biofeedback Monitor</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;500&display=swap');

  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  :root {
    --bg: #0d0f14;
    --surface: #151820;
    --border: #252a35;
    --text: #c8d0e0;
    --muted: #5a6070;
    --accent: #00e5a0;
    --warn: #ff6b4a;
    --mono: 'IBM Plex Mono', monospace;
    --sans: 'IBM Plex Sans', sans-serif;
  }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: var(--sans);
    min-height: 100vh;
    padding: 2rem;
  }

  header {
    display: flex;
    align-items: baseline;
    gap: 1rem;
    margin-bottom: 2.5rem;
    border-bottom: 1px solid var(--border);
    padding-bottom: 1rem;
  }

  header h1 {
    font-family: var(--mono);
    font-size: 0.85rem;
    font-weight: 600;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--accent);
  }

  #conn-status {
    font-family: var(--mono);
    font-size: 0.75rem;
    color: var(--muted);
  }

  #conn-status.connected { color: var(--accent); }
  #conn-status.disconnected { color: var(--warn); }

  .grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 1.5rem;
    max-width: 860px;
  }

  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 1.5rem;
  }

  .card-label {
    font-family: var(--mono);
    font-size: 0.7rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--muted);
    margin-bottom: 0.6rem;
  }

  .big-number {
    font-family: var(--mono);
    font-size: 3.5rem;
    font-weight: 600;
    line-height: 1;
    color: var(--text);
  }

  .big-number .unit {
    font-size: 1rem;
    font-weight: 400;
    color: var(--muted);
    margin-left: 0.3rem;
  }

  .calm-badge {
    display: inline-block;
    margin-top: 1rem;
    padding: 0.3rem 0.8rem;
    border-radius: 2px;
    font-family: var(--mono);
    font-size: 0.75rem;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  .calm-badge.calm { background: rgba(0,229,160,0.12); color: var(--accent); border: 1px solid rgba(0,229,160,0.3); }
  .calm-badge.not-calm { background: rgba(255,107,74,0.12); color: var(--warn); border: 1px solid rgba(255,107,74,0.3); }

  .calm-bar-wrap {
    margin-top: 1rem;
    background: var(--border);
    border-radius: 2px;
    height: 4px;
    width: 100%;
  }

  .calm-bar {
    height: 4px;
    border-radius: 2px;
    background: var(--accent);
    transition: width 0.5s ease;
  }

  .calm-bar.tense { background: var(--warn); }

  .threshold-mark-wrap {
    position: relative;
    height: 8px;
    margin-top: 2px;
  }

  .threshold-mark {
    position: absolute;
    top: 0;
    width: 2px;
    height: 8px;
    background: var(--muted);
    transform: translateX(-50%);
  }

  label {
    display: block;
    font-family: var(--mono);
    font-size: 0.7rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--muted);
    margin-bottom: 0.5rem;
    margin-top: 1.2rem;
  }

  input[type=range] {
    -webkit-appearance: none;
    width: 100%;
    height: 4px;
    border-radius: 2px;
    background: var(--border);
    outline: none;
    cursor: pointer;
  }

  input[type=range]::-webkit-slider-thumb {
    -webkit-appearance: none;
    width: 14px;
    height: 14px;
    border-radius: 50%;
    background: var(--accent);
    cursor: pointer;
  }

  input[type=number] {
    width: 100%;
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 3px;
    color: var(--text);
    font-family: var(--mono);
    font-size: 1rem;
    padding: 0.5rem 0.75rem;
    outline: none;
  }

  input[type=number]:focus { border-color: var(--accent); }

  .range-value {
    font-family: var(--mono);
    font-size: 0.85rem;
    color: var(--text);
    margin-top: 0.3rem;
  }

  button {
    display: block;
    width: 100%;
    margin-top: 1rem;
    padding: 0.65rem 1rem;
    background: transparent;
    border: 1px solid var(--accent);
    border-radius: 3px;
    color: var(--accent);
    font-family: var(--mono);
    font-size: 0.78rem;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    cursor: pointer;
    transition: background 0.15s;
  }

  button:hover { background: rgba(0,229,160,0.08); }

  button.secondary {
    border-color: var(--border);
    color: var(--muted);
  }
  button.secondary:hover { background: rgba(255,255,255,0.03); }

  .stale-warning {
    font-family: var(--mono);
    font-size: 0.7rem;
    color: var(--warn);
    margin-top: 0.5rem;
    display: none;
  }

  .data-age {
    font-family: var(--mono);
    font-size: 0.7rem;
    color: var(--muted);
    margin-top: 0.4rem;
  }
</style>
</head>
<body>
<header>
  <h1>Biofeedback Monitor</h1>
  <span id="conn-status">● connecting...</span>
</header>

<div class="grid">

  <!-- HR + Calm Score -->
  <div class="card">
    <div class="card-label">Heart Rate</div>
    <div class="big-number" id="hr-display">—<span class="unit">BPM</span></div>
    <div class="data-age" id="data-age"></div>
    <div class="stale-warning" id="stale-warning">⚠ Data is stale — check watch connection</div>

    <div style="margin-top:1.5rem">
      <div class="card-label">Calm Score <span id="calm-score-val" style="color:var(--text)">—</span></div>
      <div class="calm-bar-wrap">
        <div class="calm-bar" id="calm-bar" style="width:0%"></div>
      </div>
      <div class="threshold-mark-wrap">
        <div class="threshold-mark" id="threshold-mark" style="left:90%"></div>
      </div>
    </div>

    <span class="calm-badge not-calm" id="calm-badge">No Data</span>
  </div>

  <!-- Stats -->
  <div class="card">
    <div class="card-label">Baseline</div>
    <div class="big-number" id="baseline-display">—<span class="unit">BPM</span></div>
    <div style="margin-top:1rem">
      <div class="card-label">Threshold</div>
      <div class="big-number" style="font-size:2rem;" id="threshold-display">—</div>
    </div>
  </div>

  <!-- Set Baseline -->
  <div class="card">
    <div class="card-label">Set Baseline</div>

    <button onclick="captureBaseline()">Capture current HR as baseline</button>
    <p style="font-size:0.72rem;color:var(--muted);margin-top:0.5rem;font-family:var(--mono)">Ask the patient to sit quietly, then press.</p>

    <label>Or enter manually (BPM)</label>
    <input type="number" id="baseline-input" min="30" max="250" placeholder="e.g. 72">
    <button class="secondary" onclick="setBaseline()">Set baseline</button>
  </div>

  <!-- Calm Threshold -->
  <div class="card">
    <div class="card-label">Calm Threshold</div>
    <p style="font-size:0.72rem;color:var(--muted);font-family:var(--mono);margin-bottom:0.8rem">
      Level gates when calm score ≥ threshold.<br>
      Lower = easier to pass. Higher = stricter.
    </p>

    <label>Threshold: <span id="threshold-range-val">0.80</span></label>
    <input type="range" id="threshold-range" min="0.50" max="1.30" step="0.01" value="0.80"
           oninput="updateThresholdDisplay(this.value)">
    <button onclick="setThreshold()">Apply threshold</button>
  </div>

  <!-- Force Skip -->
  <div class="card" id="force-skip-card">
    <div class="card-label">Force Skip <span id="force-skip-badge" style="display:none;color:var(--warn);margin-left:0.5rem">● ACTIVE</span></div>
    <p style="font-size:0.72rem;color:var(--muted);font-family:var(--mono);margin-bottom:0.8rem">
      Forces <code style="color:var(--accent)">is_calm = true</code> regardless of HR.<br>
      Use to skip gates during dev/demos. Indicator stays visible until you turn it off.
    </p>
    <button id="force-skip-btn" onclick="toggleForceSkip()">Enable force skip</button>
  </div>

  <!-- Simulate Mode -->
  <div class="card">
    <div class="card-label">Simulate Mode <span id="sim-badge" style="display:none;color:var(--accent);margin-left:0.5rem">● ACTIVE</span></div>
    <p style="font-size:0.72rem;color:var(--muted);font-family:var(--mono);margin-bottom:0.8rem">
      Fakes HR with a sine wave — no watch, no Pulsoid token needed.<br>
      Sine swings ±amplitude around baseline (~20 s period).
    </p>

    <label>Amplitude: <span id="sim-amp-val">10</span> BPM</label>
    <input type="range" id="sim-amp" min="0" max="40" step="1" value="10"
           oninput="document.getElementById('sim-amp-val').textContent=this.value">

    <button id="sim-toggle-btn" onclick="toggleSimulate()">Enable simulate</button>

    <div style="margin-top:1rem">
      <div class="card-label" style="margin-bottom:0.4rem">Or pin exact HR</div>
      <input type="number" id="sim-hr-input" min="30" max="250" placeholder="e.g. 85">
      <button class="secondary" onclick="pinSimulateHR()">Set simulated HR</button>
    </div>
  </div>

</div>

<script>
  let currentThreshold = 0.80;
  let forceSkipActive = false;
  let simulateActive = false;

  function updateThresholdDisplay(val) {
    document.getElementById('threshold-range-val').textContent = parseFloat(val).toFixed(2);
  }

  function updateOverrideBadges(d) {
    // Force skip badge
    forceSkipActive = d.force_skip || false;
    const fsBadge = document.getElementById('force-skip-badge');
    const fsBtn   = document.getElementById('force-skip-btn');
    const fsCard  = document.getElementById('force-skip-card');
    fsBadge.style.display = forceSkipActive ? 'inline' : 'none';
    fsBtn.textContent = forceSkipActive ? 'Disable force skip' : 'Enable force skip';
    fsCard.style.borderColor = forceSkipActive ? 'var(--warn)' : 'var(--border)';

    // Simulate badge
    simulateActive = d.simulate_mode || false;
    const simBadge = document.getElementById('sim-badge');
    const simBtn   = document.getElementById('sim-toggle-btn');
    simBadge.style.display = simulateActive ? 'inline' : 'none';
    simBtn.textContent = simulateActive ? 'Disable simulate' : 'Enable simulate';
  }

  async function poll() {
    try {
      const res = await fetch('/status');
      const d = await res.json();

      // connection indicator
      const connEl = document.getElementById('conn-status');
      if (d.simulate_mode) {
        connEl.textContent = '● simulate mode';
        connEl.className = 'connected';
      } else if (d.pulsoid_connected) {
        connEl.textContent = '● pulsoid connected';
        connEl.className = 'connected';
      } else {
        connEl.textContent = '● pulsoid disconnected';
        connEl.className = 'disconnected';
      }

      // HR
      document.getElementById('hr-display').innerHTML =
        (d.heart_rate > 0 ? d.heart_rate : '—') + '<span class="unit">BPM</span>';

      // data age / stale
      const staleEl = document.getElementById('stale-warning');
      const ageEl = document.getElementById('data-age');
      if (d.data_age_seconds < 0) {
        ageEl.textContent = '';
        staleEl.style.display = 'block';
      } else if (d.data_age_seconds > 10) {
        staleEl.style.display = 'block';
        ageEl.textContent = '';
      } else {
        staleEl.style.display = 'none';
        ageEl.textContent = `updated ${d.data_age_seconds}s ago`;
      }

      // calm score bar (clamped 0–1.5 mapped to 0–100%)
      const score = d.calm_score;
      const barPct = Math.min((score / 1.5) * 100, 100);
      const bar = document.getElementById('calm-bar');
      bar.style.width = barPct + '%';
      bar.className = 'calm-bar' + (d.is_calm ? '' : ' tense');

      // threshold marker position on bar
      currentThreshold = d.calm_threshold;
      const markPct = Math.min((d.calm_threshold / 1.5) * 100, 100);
      document.getElementById('threshold-mark').style.left = markPct + '%';
      document.getElementById('threshold-range').value = d.calm_threshold;
      document.getElementById('threshold-range-val').textContent = d.calm_threshold.toFixed(2);

      document.getElementById('calm-score-val').textContent =
        score > 0 ? score.toFixed(2) : '—';

      // calm badge
      const badge = document.getElementById('calm-badge');
      if (d.force_skip) {
        badge.textContent = '● Force Skip ON';
        badge.className = 'calm-badge calm';
      } else if (d.heart_rate <= 0) {
        badge.textContent = 'No Data';
        badge.className = 'calm-badge not-calm';
      } else if (d.is_calm) {
        badge.textContent = '● Calm';
        badge.className = 'calm-badge calm';
      } else {
        badge.textContent = '● Not Calm';
        badge.className = 'calm-badge not-calm';
      }

      // stats
      document.getElementById('baseline-display').innerHTML =
        d.baseline + '<span class="unit">BPM</span>';
      document.getElementById('threshold-display').textContent =
        d.calm_threshold.toFixed(2);

      updateOverrideBadges(d);

    } catch(e) {
      document.getElementById('conn-status').textContent = '● server offline';
      document.getElementById('conn-status').className = 'disconnected';
    }
  }

  async function captureBaseline() {
    try {
      const res = await fetch('/capture-baseline', { method: 'POST' });
      const d = await res.json();
      if (!res.ok) { alert('Error: ' + d.detail); return; }
      alert('Baseline captured: ' + d.baseline_captured + ' BPM');
    } catch(e) { alert('Server error'); }
  }

  async function setBaseline() {
    const val = parseInt(document.getElementById('baseline-input').value);
    if (!val || val < 30 || val > 250) { alert('Enter a valid BPM between 30 and 250'); return; }
    try {
      const res = await fetch('/set-baseline', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ baseline: val })
      });
      const d = await res.json();
      if (!res.ok) { alert('Error: ' + d.detail); return; }
      document.getElementById('baseline-input').value = '';
    } catch(e) { alert('Server error'); }
  }

  async function setThreshold() {
    const val = parseFloat(document.getElementById('threshold-range').value);
    try {
      const res = await fetch('/set-threshold', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ threshold: val })
      });
      if (!res.ok) { const d = await res.json(); alert('Error: ' + d.detail); }
    } catch(e) { alert('Server error'); }
  }

  async function toggleForceSkip() {
    const endpoint = forceSkipActive ? '/force-skip/off' : '/force-skip/on';
    try {
      await fetch(endpoint, { method: 'POST' });
      await poll();
    } catch(e) { alert('Server error'); }
  }

  async function toggleSimulate() {
    if (simulateActive) {
      await fetch('/simulate/off', { method: 'POST' });
    } else {
      const amp = parseInt(document.getElementById('sim-amp').value) || 10;
      await fetch('/simulate/on', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ amplitude: amp })
      });
    }
    await poll();
  }

  async function pinSimulateHR() {
    const val = parseInt(document.getElementById('sim-hr-input').value);
    if (!val || val < 30 || val > 250) { alert('Enter a valid BPM between 30 and 250'); return; }
    try {
      const res = await fetch('/simulate/set-hr', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ heart_rate: val })
      });
      if (!res.ok) { const d = await res.json(); alert('Error: ' + d.detail); return; }
      document.getElementById('sim-hr-input').value = '';
      document.getElementById('sim-badge').style.display = 'inline';
      simulateActive = true;
      await poll();
    } catch(e) { alert('Server error'); }
  }

  // poll every 1.5 seconds
  poll();
  setInterval(poll, 1500);
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return DASHBOARD_HTML


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("  Biofeedback Server starting...")
    print(f"  Dashboard: http://localhost:{HTTP_PORT}")
    print(f"  Unity endpoint: http://localhost:{HTTP_PORT}/status")
    print("=" * 50)
    uvicorn.run(app, host="0.0.0.0", port=HTTP_PORT, log_level="warning")