import streamlit as st
import sqlite3
import os
import time
import threading
import datetime
import subprocess
import shutil
import pandas as pd
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ==========================================
# CONFIGURATION & SETUP
# ==========================================
# Update DB name to force schema refresh
DB_FILE = "asmr_automator_v6.db" 
UPLOAD_DIR = "uploads"
OUTPUT_DIR = "outputs"
SECRETS_FILE = "client_secrets.json"
TOKEN_FILE = "token.json"
SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]

# Ensure directories exist
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Page Config
st.set_page_config(page_title="ASMR Engine V7", layout="wide", page_icon="🌙")

# ==========================================
# DATABASE LAYER (SQLite)
# ==========================================
def init_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_path TEXT,
            audio_path TEXT,
            crossfade_sec REAL,
            duration_hours REAL,
            title TEXT,
            description TEXT,
            tags TEXT,
            scheduled_at TIMESTAMP,
            status_render TEXT DEFAULT 'pending',
            status_upload TEXT DEFAULT 'idle',
            youtube_id TEXT,
            output_path TEXT,
            logs TEXT DEFAULT '',
            watermark_mode TEXT DEFAULT 'none', 
            mute_original INTEGER DEFAULT 1
        )
    ''')
    conn.commit()
    conn.close()

def add_job(v_path, a_path, crossfade, hours, title, desc, tags, scheduled_at, watermark_mode, mute_original):
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    mute_val = 1 if mute_original else 0
    
    c.execute('''
        INSERT INTO jobs (video_path, audio_path, crossfade_sec, duration_hours, title, description, tags, scheduled_at, status_render, status_upload, watermark_mode, mute_original)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', 'idle', ?, ?)
    ''', (v_path, a_path, crossfade, hours, title, desc, tags, scheduled_at, watermark_mode, mute_val))
    job_id = c.lastrowid
    conn.commit()
    conn.close()
    return job_id

def update_job_status(job_id, render_status=None, upload_status=None, output_path=None, log_msg=None, youtube_id=None):
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    
    updates = []
    params = []

    if render_status:
        updates.append("status_render = ?")
        params.append(render_status)
    if upload_status:
        updates.append("status_upload = ?")
        params.append(upload_status)
    if output_path:
        updates.append("output_path = ?")
        params.append(output_path)
    if youtube_id:
        updates.append("youtube_id = ?")
        params.append(youtube_id)
    if log_msg:
        c.execute("SELECT logs FROM jobs WHERE id = ?", (job_id,))
        result = c.fetchone()
        current_log = result[0] if result else ""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_log = f"{current_log}\n[{timestamp}] {log_msg}"
        updates.append("logs = ?")
        params.append(new_log)

    if updates:
        params.append(job_id)
        query = f"UPDATE jobs SET {', '.join(updates)} WHERE id = ?"
        c.execute(query, tuple(params))
        conn.commit()
    conn.close()

def get_jobs_df():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    df = pd.read_sql_query("SELECT * FROM jobs ORDER BY id DESC", conn)
    conn.close()
    return df

def get_ready_to_upload_jobs():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM jobs WHERE status_render = 'success' AND status_upload = 'waiting_schedule'")
    rows = c.fetchall()
    conn.close()
    return [dict(row) for row in rows]

# ==========================================
# SYSTEM HELPER
# ==========================================
def open_local_folder(path):
    try:
        if not os.path.exists(path): return False
        folder_path = os.path.dirname(os.path.abspath(path))
        if os.name == 'nt': os.startfile(folder_path)
        else: subprocess.Popen(["xdg-open", folder_path])
        return True
    except Exception as e:
        print(f"Error opening folder: {e}")
        return False

# ==========================================
# YOUTUBE API LAYER
# ==========================================
def get_authenticated_service():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(SECRETS_FILE):
                raise FileNotFoundError(f"Missing {SECRETS_FILE}. Cannot authenticate.")
            flow = InstalledAppFlow.from_client_secrets_file(SECRETS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
    return build('youtube', 'v3', credentials=creds)

def upload_video_to_youtube(file_path, title, description, tags, category_id="22"):
    try:
        youtube = get_authenticated_service()
        body = {
            'snippet': {
                'title': title, 'description': description,
                'tags': tags.split(','), 'categoryId': category_id
            },
            'status': {
                'privacyStatus': 'private', 'selfDeclaredMadeForKids': False,
            }
        }
        media = MediaFileUpload(file_path, chunksize=-1, resumable=True)
        request = youtube.videos().insert(part=','.join(body.keys()), body=body, media_body=media)
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status: print(f"Uploaded {int(status.progress() * 100)}%")
        return response['id']
    except Exception as e:
        raise e

# ==========================================
# FFmpeg PROCESSING ENGINE
# ==========================================
def check_nvidia_gpu():
    try:
        subprocess.run(["nvidia-smi"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        return True
    except:
        return False

def process_asmr_video(job_id, video_path, audio_path, duration_hours, watermark_mode, mute_original):
    try:
        if not shutil.which("ffmpeg"):
            raise EnvironmentError("FFmpeg not found!")

        has_gpu = check_nvidia_gpu()
        if has_gpu:
            video_codec = "h264_nvenc"
            preset = "p1"
            encoding_msg = "🚀 GPU Detected (NVIDIA)."
        else:
            video_codec = "libx264"
            preset = "ultrafast"
            encoding_msg = "🐢 No GPU detected (CPU)."

        update_job_status(job_id, render_status="rendering", log_msg=f"1. Starting Immediate Render... {encoding_msg}")
        
        target_duration_sec = int(duration_hours * 3600)
        filename = f"asmr_{job_id}_{int(time.time())}.mp4"
        output_full_path = os.path.join(OUTPUT_DIR, filename)
        
        cmd = ["ffmpeg", "-y"]
        cmd.extend(["-stream_loop", "-1", "-i", video_path])
        cmd.extend(["-stream_loop", "-1", "-i", audio_path])
        
        # --- WATERMARK REMOVAL LOGIC ---
        video_filters = []
        if watermark_mode == 'crop_only':
            video_filters.append("crop=in_w:in_h-86:0:0")
            update_job_status(job_id, log_msg="Mode: CROP ONLY. Cutting bottom 86px.")
        elif watermark_mode == 'blur':
            video_filters.append("delogo=x=0:y=h-86:w=w:h=86")
            update_job_status(job_id, log_msg="Mode: BLUR. Blurring bottom 86px.")
        elif watermark_mode == 'zoom_tl':
            video_filters.append("crop=in_w-150:in_h-86:0:0,scale=1920:1080:flags=lanczos")
            update_job_status(job_id, log_msg="Mode: ZOOM TOP-LEFT. Cropping bottom-right and rescaling to 1080p.")

        if video_filters:
            cmd.extend(["-vf", ",".join(video_filters)])
            
        if mute_original:
            cmd.extend(["-map", "0:v:0", "-map", "1:a:0"])
        else:
            cmd.extend(["-filter_complex", "[0:a][1:a]amix=inputs=2:duration=shortest[aout]", "-map", "0:v:0", "-map", "[aout]"])

        cmd.extend([
            "-t", str(target_duration_sec),
            "-c:v", video_codec, "-preset", preset,
            "-c:a", "aac", "-b:a", "192k",
            output_full_path
        ])
        
        process = subprocess.run(cmd, capture_output=True, text=True)
        
        if process.returncode != 0:
            raise Exception(f"FFmpeg Error: {process.stderr}")
            
        update_job_status(
            job_id, 
            render_status="success", 
            upload_status="waiting_schedule",
            output_path=output_full_path, 
            log_msg="2. Render Finished. Now Waiting for Schedule Time..."
        )
        
    except Exception as e:
        update_job_status(job_id, render_status="failed", log_msg=f"Rendering Failed: {str(e)}")
        print(f"Error rendering job {job_id}: {e}")

# ==========================================
# BACKGROUND SCHEDULER
# ==========================================
def scheduler_loop():
    while True:
        try:
            jobs = get_ready_to_upload_jobs()
            for job in jobs:
                scheduled_time = datetime.datetime.fromisoformat(job['scheduled_at'])
                current_time = datetime.datetime.now()
                
                if current_time >= scheduled_time:
                    update_job_status(job['id'], upload_status="uploading", log_msg="3. Schedule Reached. Uploading...")
                    try:
                        vid_id = upload_video_to_youtube(
                            job['output_path'], job['title'], job['description'], job['tags']
                        )
                        update_job_status(job['id'], upload_status="success", youtube_id=vid_id, log_msg=f"4. Upload Success! ID: {vid_id}")
                    except Exception as e:
                        update_job_status(job['id'], upload_status="failed", log_msg=f"Upload Failed: {str(e)}")
            time.sleep(20)
        except Exception as e:
            time.sleep(20)

@st.cache_resource
def start_scheduler():
    init_db()
    t = threading.Thread(target=scheduler_loop, daemon=True)
    t.start()
    return t

start_scheduler()

# ==========================================
# UI
# ==========================================
def ui_upload_tab():
    st.header("1. Create, Render & Schedule")
    has_gpu = check_nvidia_gpu()
    if has_gpu: st.success("✅ NVIDIA GPU Detected")
    else: st.warning("⚠️ No GPU Detected (CPU Mode)")

    # --- FIX: Session State for Time Picker ---
    # Ini mencegah jam reset setiap kali ada interaksi UI
    if 'default_schedule_time' not in st.session_state:
        st.session_state.default_schedule_time = datetime.datetime.now().time()

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Media & Edit")
        uploaded_video = st.file_uploader("Video Loop (MP4)", type=['mp4', 'mov'])
        uploaded_audio = st.file_uploader("Audio Track (MP3/WAV)", type=['mp3', 'wav', 'aac'])
        st.markdown("---")
        
        watermark_mode = st.selectbox(
            "Watermark Removal Mode", 
            ["none", "zoom_tl", "crop_only", "blur"],
            format_func=lambda x: {
                "none": "⛔ None (Original)",
                "zoom_tl": "✨ Zoom Top-Left (Recommended)",
                "crop_only": "✂️ Crop Bottom Only",
                "blur": "💧 Blur Bottom"
            }[x],
            index=1
        )
        if watermark_mode == "zoom_tl":
            st.caption("ℹ️ *Crop kanan-bawah, lalu zoom agar Full HD.*")

        remove_audio = st.toggle("🔇 Hapus Audio Bawaan Video", value=True)
        st.markdown("---")
        duration = st.number_input("Duration (Hours)", 0.1, 24.0, 1.0, 0.5)

    with col2:
        st.subheader("Metadata")
        title = st.text_input("Title", "ASMR Sleep Loop")
        desc = st.text_area("Description", "Relaxing video...\n#asmr")
        tags = st.text_input("Tags", "asmr,sleep")
        st.divider()
        
        st.write("**Set Upload Schedule**")
        c_d, c_t = st.columns(2)
        s_date = c_d.date_input("Date", datetime.date.today())
        
        # Gunakan session_state untuk default value
        s_time = c_t.time_input("Time", value=st.session_state.default_schedule_time)

    if st.button("🚀 Render Now (Upload Later)", type="primary"):
        if uploaded_video and uploaded_audio:
            v_path = os.path.join(UPLOAD_DIR, uploaded_video.name)
            a_path = os.path.join(UPLOAD_DIR, uploaded_audio.name)
            with open(v_path, "wb") as f: f.write(uploaded_video.getbuffer())
            with open(a_path, "wb") as f: f.write(uploaded_audio.getbuffer())
            
            # Combine Date and Time
            s_dt = datetime.datetime.combine(s_date, s_time)
            
            job_id = add_job(v_path, a_path, 0, duration, title, desc, tags, s_dt, watermark_mode, remove_audio)
            t = threading.Thread(target=process_asmr_video, args=(job_id, v_path, a_path, duration, watermark_mode, remove_audio))
            t.start()
            
            st.success(f"Job #{job_id} Started! Rendering now. Upload scheduled at {s_dt}.")
        else:
            st.error("Upload files first.")

def ui_manager_tab():
    st.header("Status Manager")
    if st.button("🔄 Refresh Data"): st.rerun()
    
    df = get_jobs_df()
    if not df.empty:
        st.dataframe(df[['id', 'title', 'status_render', 'status_upload', 'scheduled_at']], use_container_width=True, hide_index=True)
        st.divider()
        
        st.subheader("🔎 Job Details")
        sel_id = st.selectbox("Select Job ID to view:", df['id'].tolist())
        
        if sel_id:
            job = df[df['id'] == sel_id].iloc[0]
            c1, c2, c3 = st.columns([1, 1, 1])
            
            with c1:
                st.write("#### Monitor")
                r_status = job['status_render']
                u_status = job['status_upload']
                
                if r_status == 'success': st.success(f"Render: ✅ DONE")
                elif r_status == 'rendering': st.warning(f"Render: ⚙️ PROCESSING...")
                elif r_status == 'failed': st.error(f"Render: ❌ FAILED")
                else: st.info(f"Render: {r_status}")
                
                if u_status == 'success': st.success(f"Upload: ✅ DONE")
                elif u_status == 'uploading': st.warning(f"Upload: ☁️ UPLOADING...")
                elif u_status == 'waiting_schedule': 
                    st.info(f"Upload: ⏳ WAITING SCHEDULE")
                    st.caption(f"At: {job['scheduled_at']}")
                elif u_status == 'failed': st.error(f"Upload: ❌ FAILED")
                else: st.write(f"Upload: ⏸️ Idle")

                st.markdown("---")
                st.write("📂 **Source Files**")
                if os.path.exists(job['video_path']):
                    if st.button("📂 Source Video", key=f"v_{sel_id}"): open_local_folder(job['video_path'])
                if os.path.exists(job['audio_path']):
                    if st.button("📂 Source Audio", key=f"a_{sel_id}"): open_local_folder(job['audio_path'])
                
                st.markdown("---")
                if r_status == 'success' and job['output_path'] and os.path.exists(job['output_path']):
                    st.write("**Output Result:**")
                    st.video(job['output_path'])
                    if st.button(f"📂 Open Output Folder", key=f"o_{sel_id}"): open_local_folder(job['output_path'])

            with c2:
                st.write("📋 **Metadata**")
                st.text_input("Title", value=job['title'], key=f"t_{sel_id}")
                st.text_area("Description", value=job['description'], height=150, key=f"d_{sel_id}")
                st.text_area("Tags", value=job['tags'], key=f"tg_{sel_id}")
                
            with c3:
                st.write("📝 **Logs**")
                st.text_area("Logs", value=job['logs'], height=350, key=f"l_{sel_id}", disabled=True)
    else:
        st.info("No jobs yet.")

def main():
    st.title("📹 ASMR Engine V7 (Stable Time Picker)")
    t1, t2 = st.tabs(["Create", "Manage"])
    with t1: ui_upload_tab()
    with t2: ui_manager_tab()

if __name__ == "__main__":
    main()