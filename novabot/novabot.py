+#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NovaBot v3.0 - Production Final
Telegram -> WordPress Otomasyon Sistemi
Tek dosya, eksiksiz, production-ready
"""

import os
import sys
import time
import signal
import logging
import sqlite3
import hashlib
import asyncio
import smtplib
import threading
import schedule
import requests
import random
import re
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from functools import wraps

try:
    from telethon import TelegramClient as TelethonClient
    from telethon.tl.types import MessageMediaDocument
    from telethon.errors import FloodWaitError
except ImportError:
    print("pip install telethon")
    sys.exit(1)

try:
    from flask import Flask, render_template_string, request, jsonify, redirect, session
except ImportError:
    print("pip install flask")
    sys.exit(1)

try:
    from unidecode import unidecode
except ImportError:
    print("pip install unidecode")
    sys.exit(1)

load_dotenv("config.env")


# ════════════════════════════════════════════════════════════════
# 1. LOGGING
# ════════════════════════════════════════════════════════════════

def setup_logging():
    os.makedirs("logs", exist_ok=True)
    handler = RotatingFileHandler(
        'logs/novabot.log',
        maxBytes=5 * 1024 * 1024,
        backupCount=5
    )
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[handler, logging.StreamHandler()]
    )
    return logging.getLogger("novabot")


# ════════════════════════════════════════════════════════════════
# 2. CONFIG VALIDATOR
# ════════════════════════════════════════════════════════════════

def validate_config(logger):
    required = {
        'TELEGRAM_API_ID':32880234 
        'TELEGRAM_API_HASH': eef613f739133d7722a7f6a7a372b5b1
        'TELEGRAM_PHONE': +905428920069
        'TELEGRAM_KANAL': https://t.me/+s2rUVkv9I-diOTRk
        'WP_URL': https://madamx.net
        'WP_KULLANICI': /wp-Novabot
        'WP_APP_PASSWORD': "23nP 0gPD MNiX VyNc yrOK qJIJ"
        'SECRET_KEY': NovaBot_MadamX_2026_Guvenli_Anahtar_!*
        'WEB_PANEL_PASSWORD': Faruk12345
    }

    missing = []
    for key, desc in required.items():
        val = os.getenv(key, "").strip()
        if not val:
            missing.append(f"  {key} ({desc})")

    if missing:
        print("\n" + "=" * 60)
        print("CONFIG HATASI - Eksik alanlar:")
        for m in missing:
            print(m)
        print("\nconfig.env dosyasini duzenleyin!")
        print("WordPress App Password icin:")
        print("  WordPress > Users > Application Passwords")
        print("=" * 60 + "\n")
        sys.exit(1)

    toplam = 0
    for i in range(1, 9):
        limit = os.getenv(f"KATEGORI{i}_LIMIT", "0")
        try:
            toplam += int(limit)
        except:
            pass

    gunluk_limit = int(os.getenv("GUNLUK_LIMIT", 48))
    if toplam > 0 and toplam != gunluk_limit:
        logger.warning(f"Kategori limitleri toplami ({toplam}) GUNLUK_LIMIT ({gunluk_limit}) ile eslesmiyor!")

    logger.info("Config dogrulama gecti")


# ════════════════════════════════════════════════════════════════
# 3. QUEUE MANAGER
# ════════════════════════════════════════════════════════════════

class QueueManager:
    def __init__(self, logger):
        self.logger = logger
        self.db_path = "novabot.db"
        self.gunluk_limit = int(os.getenv("GUNLUK_LIMIT", 48))
        self.max_ust_uste = 2

        self.kategori_limitler = {}
        for i in range(1, 9):
            limit = os.getenv(f"KATEGORI{i}_LIMIT")
            if limit:
                self.kategori_limitler[f"kategori{i}"] = int(limit)

        self.init_db()
        self.recover_publishing_state()

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def init_db(self):
        conn = self._get_conn()
        c = conn.cursor()

        c.execute("""
            CREATE TABLE IF NOT EXISTS queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_msg_id TEXT UNIQUE,
                video_hash TEXT,
                baslik TEXT,
                kategori_anahtar TEXT,
                kategori_adi TEXT,
                hashtags TEXT,
                file_id TEXT,
                boyut_mb REAL,
                status TEXT DEFAULT 'pending',
                yayin_zamani TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                published_at TEXT,
                retry_count INTEGER DEFAULT 0
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS kategori_sayac (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                son_kategori TEXT,
                ust_uste_sayac INTEGER DEFAULT 0,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        c.execute("SELECT COUNT(*) FROM kategori_sayac")
        if c.fetchone()[0] == 0:
            c.execute("INSERT INTO kategori_sayac (id, son_kategori, ust_uste_sayac) VALUES (1, '', 0)")

        conn.commit()
        conn.close()
        self.logger.info("DB hazir (WAL modu)")

    def recover_publishing_state(self):
        """Crash recovery: publishing -> pending"""
        try:
            conn = self._get_conn()
            c = conn.cursor()
            c.execute("UPDATE queue SET status = 'pending' WHERE status = 'publishing'")
            recovered = c.rowcount
            conn.commit()
            conn.close()
            if recovered > 0:
                self.logger.info(f"Crash recovery: {recovered} video pending'e alindi")
        except Exception as e:
            self.logger.error(f"Recovery hatasi: {e}")

    def kategori_limit_doldu_mu(self, kategori_anahtar):
        if not kategori_anahtar or kategori_anahtar not in self.kategori_limitler:
            return False
        try:
            conn = self._get_conn()
            c = conn.cursor()
            today = datetime.now().strftime("%Y-%m-%d")
            c.execute("""
                SELECT COUNT(*) FROM queue
                WHERE DATE(published_at) = ? AND kategori_anahtar = ? AND status = 'published'
            """, (today, kategori_anahtar))
            bugun = c.fetchone()[0]
            limit = self.kategori_limitler[kategori_anahtar]
            conn.close()
            return bugun >= limit
        except Exception as e:
            self.logger.error(f"Kategori limit hatasi: {e}")
            return False

    def add_video(self, video):
        try:
            conn = self._get_conn()
            c = conn.cursor()

            c.execute("SELECT id FROM queue WHERE telegram_msg_id = ?", (video['telegram_msg_id'],))
            if c.fetchone():
                conn.close()
                return False

            c.execute("SELECT id FROM queue WHERE video_hash = ?", (video['video_hash'],))
            if c.fetchone():
                conn.close()
                return False

            today = datetime.now().strftime("%Y-%m-%d")
            c.execute("SELECT COUNT(*) FROM queue WHERE DATE(published_at) = ? AND status = 'published'", (today,))
            bugun = c.fetchone()[0]

            if bugun >= self.gunluk_limit:
                yayin_zamani = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
            else:
                yayin_zamani = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            c.execute("""
                INSERT INTO queue (
                    telegram_msg_id, video_hash, baslik,
                    kategori_anahtar, kategori_adi, hashtags,
                    file_id, boyut_mb, yayin_zamani, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
            """, (
                video['telegram_msg_id'],
                video.get('video_hash', ''),
                video['baslik'],
                video['kategori_anahtar'],
                video['kategori_adi'],
                video['hashtags'],
                video['file_id'],
                video.get('boyut_mb', 0),
                yayin_zamani
            ))

            conn.commit()
            conn.close()
            return True

        except Exception as e:
            self.logger.error(f"Queue ekleme hatasi: {e}")
            return False

    def get_next_video(self):
        try:
            conn = self._get_conn()
            c = conn.cursor()

            c.execute("SELECT son_kategori, ust_uste_sayac FROM kategori_sayac WHERE id = 1")
            row = c.fetchone()
            son_kategori, ust_uste_sayac = row if row else ('', 0)

            c.execute("SELECT * FROM queue WHERE status = 'pending' ORDER BY yayin_zamani ASC")
            tum_videolar = c.fetchall()

            secilen = None
            ust_uste_doldu = ust_uste_sayac >= self.max_ust_uste and son_kategori
            kategori_doldu = self.kategori_limit_doldu_mu(son_kategori)

            if ust_uste_doldu or kategori_doldu:
                for r in tum_videolar:
                    kat = r[4]
                    if kat != son_kategori and not self.kategori_limit_doldu_mu(kat):
                        secilen = r
                        break
                if not secilen:
                    for r in tum_videolar:
                        if not self.kategori_limit_doldu_mu(r[4]):
                            secilen = r
                            break
            else:
                for r in tum_videolar:
                    if not self.kategori_limit_doldu_mu(r[4]):
                        secilen = r
                        break

            if not secilen and tum_videolar:
                secilen = tum_videolar[0]
                self.logger.warning("Tum kategori limitleri doldu, fallback")

            if secilen:
                video = {
                    'id': secilen[0],
                    'telegram_msg_id': secilen[1],
                    'video_hash': secilen[2],
                    'baslik': secilen[3],
                    'kategori_anahtar': secilen[4],
                    'kategori_adi': secilen[5],
                    'hashtags': secilen[6],
                    'file_id': secilen[7],
                    'boyut_mb': secilen[8]
                }

                yeni_sayac = ust_uste_sayac + 1 if video['kategori_anahtar'] == son_kategori else 1

                # Atomic: secim + publishing state ayni transaction
                c.execute("UPDATE queue SET status = 'publishing' WHERE id = ?", (video['id'],))
                c.execute("""
                    UPDATE kategori_sayac
                    SET son_kategori = ?, ust_uste_sayac = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = 1
                """, (video['kategori_anahtar'], yeni_sayac))

                conn.commit()
                conn.close()
                return video

            conn.close()
            return None

        except Exception as e:
            self.logger.error(f"get_next_video hatasi: {e}")
            return None

    def mark_published(self, video_id):
        try:
            conn = self._get_conn()
            c = conn.cursor()
            c.execute("""
                UPDATE queue SET status = 'published', published_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (video_id,))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            self.logger.error(f"mark_published hatasi: {e}")
            return False

    def mark_failed(self, video_id):
        try:
            conn = self._get_conn()
            c = conn.cursor()
            c.execute("SELECT retry_count FROM queue WHERE id = ?", (video_id,))
            retry_count = c.fetchone()[0]
            if retry_count < 2:
                c.execute("""
                    UPDATE queue SET status = 'pending', retry_count = retry_count + 1
                    WHERE id = ?
                """, (video_id,))
                self.logger.info(f"Retry {retry_count + 2}/3")
            else:
                c.execute("UPDATE queue SET status = 'failed' WHERE id = ?", (video_id,))
                self.logger.error(f"Video kalici basarisiz: {video_id}")
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            self.logger.error(f"mark_failed hatasi: {e}")
            return False

    def cleanup_old_records(self, gun=30):
        """30 gunden eski published kayitlari temizle"""
        try:
            conn = self._get_conn()
            c = conn.cursor()
            sinir = (datetime.now() - timedelta(days=gun)).strftime("%Y-%m-%d")
            c.execute("DELETE FROM queue WHERE status = 'published' AND DATE(published_at) < ?", (sinir,))
            silinen = c.rowcount
            conn.commit()
            conn.close()
            if silinen > 0:
                self.logger.info(f"DB temizlendi: {silinen} eski kayit silindi")
        except Exception as e:
            self.logger.error(f"Cleanup hatasi: {e}")

    def get_stats(self):
        try:
            conn = self._get_conn()
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM queue WHERE status = 'pending'")
            pending = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM queue WHERE status = 'published'")
            published = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM queue WHERE status = 'failed'")
            failed = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM queue WHERE status = 'publishing'")
            publishing = c.fetchone()[0]
            today = datetime.now().strftime("%Y-%m-%d")
            c.execute("SELECT COUNT(*) FROM queue WHERE DATE(published_at) = ?", (today,))
            today_pub = c.fetchone()[0]
            conn.close()
            return {
                'pending': pending, 'published': published,
                'failed': failed, 'publishing': publishing, 'today': today_pub
            }
        except Exception as e:
            self.logger.error(f"Stats hatasi: {e}")
            return {}

    def get_kategori_stats(self):
        try:
            conn = self._get_conn()
            c = conn.cursor()
            today = datetime.now().strftime("%Y-%m-%d")
            c.execute("""
                SELECT kategori_anahtar, COUNT(*) FROM queue
                WHERE DATE(published_at) = ? AND status = 'published'
                GROUP BY kategori_anahtar
            """, (today,))
            sonuclar = {}
            for row in c.fetchall():
                kat, sayi = row
                limit = self.kategori_limitler.get(kat, 0)
                sonuclar[kat] = {
                    'yayinlanan': sayi,
                    'limit': limit,
                    'kalan': max(0, limit - sayi)
                }
            conn.close()
            return sonuclar
        except Exception as e:
            self.logger.error(f"Kategori stats hatasi: {e}")
            return {}


# ════════════════════════════════════════════════════════════════
# 4. TELEGRAM CLIENT
# ════════════════════════════════════════════════════════════════

class TelegramClientWrapper:
    def __init__(self, logger):
        self.logger = logger
        self.api_id = os.getenv("TELEGRAM_API_ID")
        self.api_hash = os.getenv("TELEGRAM_API_HASH")
        self.phone = os.getenv("TELEGRAM_PHONE")
        self.kanal = os.getenv("TELEGRAM_KANAL")
        self.max_dosya_mb = int(os.getenv("MAX_DOSYA_MB", 2000))

        self.kategoriler = {}
        for i in range(1, 9):
            deger = os.getenv(f"KATEGORI{i}")
            if deger:
                self.kategoriler[f"kategori{i}"] = deger.strip()

        self.client = None
        self.son_msg_id = self._load_son_msg_id()

    def _load_son_msg_id(self):
        try:
            if os.path.exists("son_msg_id.txt"):
                with open("son_msg_id.txt", "r") as f:
                    return int(f.read().strip())
        except:
            pass
        return 0

    def _save_son_msg_id(self, msg_id):
        try:
            with open("son_msg_id.txt", "w") as f:
                f.write(str(msg_id))
        except Exception as e:
            self.logger.error(f"son_msg_id kayit hatasi: {e}")

    async def _connect(self):
        self.client = TelethonClient('novabot_session', self.api_id, self.api_hash)
        await self.client.start(phone=self.phone)
        self.logger.info("Telegram baglantisi kuruldu")

    async def _fetch_async(self):
        if not self.client:
            await self._connect()

        videolar = []

        try:
            async for message in self.client.iter_messages(self.kanal, limit=20):
                if message.id <= self.son_msg_id:
                    continue
                if not message.media or not isinstance(message.media, MessageMediaDocument):
                    continue
                if not message.media.document.mime_type.startswith('video/'):
                    continue

                boyut_mb = message.media.document.size / (1024 * 1024)
                if boyut_mb > self.max_dosya_mb:
                    self.logger.warning(f"Dosya cok buyuk: {boyut_mb:.1f} MB")
                    continue

                parsed = self._parse_caption(message.message)
                video_hash = hashlib.md5(
                    f"{message.media.document.id}_{parsed['baslik']}".encode()
                ).hexdigest()

                videolar.append({
                    'telegram_msg_id': str(message.id),
                    'video_hash': video_hash,
                    'baslik': parsed['baslik'],
                    'kategori_anahtar': parsed['kategori_anahtar'],
                    'kategori_adi': parsed['kategori_adi'],
                    'hashtags': ','.join(parsed['hashtags']),
                    'file_id': str(message.media.document.id),
                    'boyut_mb': round(boyut_mb, 1)
                })

                self.logger.info(f"Video: [{parsed['kategori_adi']}] {parsed['baslik']}")

            if videolar:
                yeni_id = max(int(v['telegram_msg_id']) for v in videolar)
                self.son_msg_id = yeni_id
                self._save_son_msg_id(yeni_id)

            return videolar

        except FloodWaitError as e:
            self.logger.warning(f"FloodWait: {e.seconds}s bekleniyor")
            await asyncio.sleep(e.seconds)
            return []
        except Exception as e:
            self.logger.error(f"Telegram fetch hatasi: {e}")
            return []

    def get_new_videos(self):
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(self._fetch_async())
            loop.close()
            return result
        except Exception as e:
            self.logger.error(f"Telegram wrapper hatasi: {e}")
            return []

    def _parse_caption(self, caption):
        if not caption:
            return {'baslik': 'Video', 'kategori_anahtar': 'genel', 'kategori_adi': 'Genel', 'hashtags': []}

        satirlar = caption.strip().split('\n')
        baslik = satirlar[0].strip() if satirlar else 'Video'
        kategori_anahtar = 'genel'
        kategori_adi = 'Genel'
        hashtags = []

        if len(satirlar) > 1:
            ikinci = satirlar[1].lower()
            kelimeler = ikinci.split()

            bulundu = False
            for kelime in kelimeler:
                if kelime.startswith('#kategori'):
                    kat = kelime.replace('#', '')
                    if kat in self.kategoriler:
                        kategori_anahtar = kat
                        kategori_adi = self.kategoriler[kat]
                        bulundu = True
                        break

            if not bulundu:
                for anahtar, ad in self.kategoriler.items():
                    if ad.lower() in ikinci:
                        kategori_anahtar = anahtar
                        kategori_adi = ad
                        break

            for kelime in kelimeler:
                if kelime.startswith('#') and not kelime.startswith('#kategori'):
                    temiz = kelime.replace('#', '').strip()
                    if temiz and temiz.lower() != kategori_adi.lower() and len(hashtags) < 5:
                        hashtags.append(temiz)

        return {'baslik': baslik, 'kategori_anahtar': kategori_anahtar, 'kategori_adi': kategori_adi, 'hashtags': hashtags}


# ════════════════════════════════════════════════════════════════
# 5. TITLE GENERATOR
# ════════════════════════════════════════════════════════════════

class TitleGenerator:
    def __init__(self, logger):
        self.logger = logger
        self.site_adi = os.getenv("SITE_ADI", "NovaBot")

        self.formats = {
            'dizi':      ["{b} - {s} Ozel", "{b} | {s} Izle", "{b} - Full Bolum", "{b} | HD Izle", "{b} - {s}"],
            'film':      ["{b} - Full Film", "{b} | {s} HD", "{b} - Turkce Alt", "{b} | HD Izle", "{b} - {s}"],
            'teknoloji': ["{b} - Inceleme", "{b} | {s}", "{b} - Detayli", "{b} | Test", "{b} - {s}"],
            'spor':      ["{b} - Ozet", "{b} | {s}", "{b} - Gol", "{b} | Mac Ozeti", "{b} - {s}"],
            'muzik':     ["{b} - {s}", "{b} | Sarki", "{b} - Dinle", "{b} | Muzik", "{b} - {s}"],
            'oyun':      ["{b} - Gameplay", "{b} | {s}", "{b} - Inceleme", "{b} | Oyun", "{b} - {s}"],
            'yasam':     ["{b} - {s}", "{b} | Yasam", "{b} - Detay", "{b} | Ozel", "{b} - Oneri"],
            'haber':     ["{b} - Son Dakika", "{b} | {s}", "{b} - Detay", "{b} | Haber", "{b} - {s}"],
            'genel':     ["{b} - {s}", "{b} | {s}", "{b} - Izle", "{b} | Video", "{b} - Ozel"],
        }

    def _temizle(self, baslik):
        baslik = ' '.join(baslik.split())
        emoji = re.compile("["
            u"\U0001F600-\U0001F64F"
            u"\U0001F300-\U0001F5FF"
            u"\U0001F680-\U0001F6FF"
            u"\U0001F1E0-\U0001F1FF"
            u"\U00002702-\U000027B0"
            u"\U000024C2-\U0001F251"
            "]+", flags=re.UNICODE)
        return emoji.sub('', baslik).strip()

    def _kisalt(self, baslik, max_len):
        if len(baslik) <= max_len:
            return baslik
        kelimeler = baslik.split()
        result, toplam = [], 0
        for k in kelimeler:
            if toplam + len(k) + 1 > max_len:
                break
            result.append(k)
            toplam += len(k) + 1
        return ' '.join(result)

    def generate(self, baslik, kategori_adi):
        try:
            baslik = self._temizle(baslik)
            kat = kategori_adi.lower()
            fmt_list = self.formats.get(kat, self.formats['genel'])
            fmt = random.choice(fmt_list)

            max_b = 50 - len(self.site_adi) - 5 if '{s}' in fmt else 50
            b = self._kisalt(baslik, max_b)
            title = fmt.format(b=b, s=self.site_adi)

            if len(title) > 60:
                title = f"{self._kisalt(baslik, 40)} - {self.site_adi}"
            if len(title) < 55:
                title = f"{b} | {self.site_adi}"

            return title
        except Exception as e:
            self.logger.error(f"Title hatasi: {e}")
            return f"{baslik[:45]} - {self.site_adi}"


# ════════════════════════════════════════════════════════════════
# 6. WORDPRESS CLIENT
# ════════════════════════════════════════════════════════════════

class WordPressClient:
    def __init__(self, logger):
        self.logger = logger
        self.wp_url = os.getenv("WP_URL", "").rstrip('/')
        self.wp_kullanici = os.getenv("WP_KULLANICI")
        self.wp_sifre = os.getenv("WP_APP_PASSWORD")
        self.site_adi = os.getenv("SITE_ADI", "NovaBot")
        self.kanal = os.getenv("TELEGRAM_KANAL", "").lstrip('@')
        self.dry_run = os.getenv("DRY_RUN", "false").lower() == "true"
        self.title_gen = TitleGenerator(logger)
        self.max_retry = 3
        self._tag_cache = {}
        self._kat_cache = {}

    def _auth(self):
        return (self.wp_kullanici, self.wp_sifre)

    def _kategori_id(self, kategori_adi):
        if kategori_adi in self._kat_cache:
            return self._kat_cache[kategori_adi]
        try:
            r = requests.get(
                f"{self.wp_url}/wp-json/wp/v2/categories",
                params={"search": kategori_adi},
                auth=self._auth(), timeout=30
            )
            if r.status_code == 200 and r.json():
                kid = r.json()[0]["id"]
                self._kat_cache[kategori_adi] = kid
                return kid

            r = requests.post(
                f"{self.wp_url}/wp-json/wp/v2/categories",
                json={"name": kategori_adi.capitalize()},
                auth=self._auth(), timeout=30
            )
            if r.status_code == 201:
                kid = r.json()["id"]
                self._kat_cache[kategori_adi] = kid
                return kid
        except Exception as e:
            self.logger.error(f"Kategori ID hatasi: {e}")
        return None

    def _tag_id(self, tag_adi):
        if tag_adi in self._tag_cache:
            return self._tag_cache[tag_adi]
        try:
            r = requests.get(
                f"{self.wp_url}/wp-json/wp/v2/tags",
                params={"search": tag_adi},
                auth=self._auth(), timeout=20
            )
            if r.status_code == 200 and r.json():
                tid = r.json()[0]["id"]
                self._tag_cache[tag_adi] = tid
                return tid

            r = requests.post(
                f"{self.wp_url}/wp-json/wp/v2/tags",
                json={"name": tag_adi},
                auth=self._auth(), timeout=20
            )
            if r.status_code == 201:
                tid = r.json()["id"]
                self._tag_cache[tag_adi] = tid
                return tid
        except Exception as e:
            self.logger.error(f"Tag ID hatasi ({tag_adi}): {e}")
        return None

    def _slug(self, baslik):
        s = unidecode(baslik).lower()
        s = re.sub(r'[^a-z0-9\s-]', '', s)
        s = re.sub(r'[\s-]+', '-', s).strip('-')
        return s[:50]

    def _slug_var_mi(self, slug):
        try:
            r = requests.get(
                f"{self.wp_url}/wp-json/wp/v2/posts",
                params={"slug": slug},
                auth=self._auth(), timeout=20
            )
            if r.status_code == 200 and r.json():
                return True
        except:
            pass
        return False

    def _embed(self, telegram_msg_id, baslik):
        return f"""
<div class="novabot-player" style="max-width:100%;background:#000;border-radius:12px;margin:20px 0;overflow:hidden;">
    <iframe
        src="https://t.me/{self.kanal}/{telegram_msg_id}?embed=1"
        width="100%"
        height="600"
        frameborder="0"
        scrolling="no"
        style="border:none;">
    </iframe>
    <div style="padding:15px;background:#1a1a1a;">
        <p style="color:#fff;margin:0;font-size:14px;"><strong>{baslik}</strong></p>
    </div>
</div>"""

    def _seo_aciklama(self, baslik, kategori_adi, hashtags):
        ht = hashtags.split(',') if hashtags else []
        ht_text = ', '.join(ht[:3]) if ht else kategori_adi
        return (
            f"{baslik} - {self.site_adi} uzerinde en iyi "
            f"{kategori_adi.lower()} iceriklerini izleyin. "
            f"{ht_text} ve daha fazlasi icin {self.site_adi}'i takip edin."
        )

    def publish_video(self, video):
        if self.dry_run:
            self.logger.info(f"[DRY RUN] {video['baslik']}")
            return True

        retry = 0
        while retry < self.max_retry:
            try:
                baslik = self.title_gen.generate(video['baslik'], video['kategori_adi'])
                aciklama = self._seo_aciklama(video['baslik'], video['kategori_adi'], video['hashtags'])

                tag_adlari = []
                if video['hashtags']:
                    tag_adlari = [h.strip() for h in video['hashtags'].split(',') if h.strip()]
                tag_adlari.append(video['kategori_adi'].lower())
                tag_adlari.append(self.site_adi.lower())
                tag_adlari = list(set(tag_adlari))[:10]
                tag_ids = [tid for t in tag_adlari if (tid := self._tag_id(t))]

                slug = self._slug(video['baslik'])
                if self._slug_var_mi(slug):
                    slug = f"{slug}-{int(time.time())}"

                player = self._embed(video['telegram_msg_id'], video['baslik'])

                icerik = f"""{player}

<div style="margin-top:20px;padding:20px;background:#f5f5f5;border-radius:8px;">
    <p style="font-size:15px;line-height:1.6;">{aciklama}</p>
    <p style="margin-top:10px;color:#666;"><strong>Kategori:</strong> {video['kategori_adi'].capitalize()}</p>
</div>"""

                kat_id = self._kategori_id(video['kategori_adi'])

                veri = {
                    "title": baslik,
                    "content": icerik,
                    "excerpt": aciklama[:150],
                    "status": "publish",
                    "slug": slug,
                    "tags": tag_ids
                }
                if kat_id:
                    veri["categories"] = [kat_id]

                r = requests.post(
                    f"{self.wp_url}/wp-json/wp/v2/posts",
                    json=veri, auth=self._auth(), timeout=60
                )

                if r.status_code == 201:
                    self.logger.info(f"Yayinlandi: {r.json()['link']}")
                    return True

                if r.status_code == 429:
                    self.logger.warning("WordPress rate limit, 60s bekleniyor")
                    time.sleep(60)
                    continue

                self.logger.error(f"WP {r.status_code}: {r.text[:200]}")
                retry += 1
                if retry < self.max_retry:
                    delay = 5 * (2 ** (retry - 1))
                    self.logger.info(f"Retry {retry}/{self.max_retry} ({delay}s)")
                    time.sleep(delay)

            except Exception as e:
                self.logger.error(f"Yayinlama hatasi: {e}")
                retry += 1
                if retry < self.max_retry:
                    delay = 5 * (2 ** (retry - 1))
                    time.sleep(delay)

        return False


# ════════════════════════════════════════════════════════════════
# 7. EMAIL REPORTER
# ════════════════════════════════════════════════════════════════

class EmailReporter:
    def __init__(self, logger, queue_manager):
        self.logger = logger
        self.queue = queue_manager
        self.email = os.getenv("BILDIRIM_EMAIL")
        self.smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", 587))
        self.smtp_kullanici = os.getenv("SMTP_KULLANICI")
        self.smtp_sifre = os.getenv("SMTP_SIFRE")

    def _ayarlar_gecerli(self):
        return all([self.email, self.smtp_kullanici, self.smtp_sifre])

    def send_daily_report(self):
        if not self._ayarlar_gecerli():
            self.logger.info("Email ayarlari eksik, rapor atlandi")
            return False

        try:
            stats = self.queue.get_stats()
            kat_stats = self.queue.get_kategori_stats()
            today = datetime.now().strftime("%Y-%m-%d")

            kat_rows = "".join(
                f"<tr>"
                f"<td style='padding:8px;border:1px solid #ddd;'>{k}</td>"
                f"<td style='padding:8px;border:1px solid #ddd;text-align:center;'>{v['yayinlanan']}/{v['limit']}</td>"
                f"<td style='padding:8px;border:1px solid #ddd;text-align:center;'>{v['kalan']}</td>"
                f"</tr>"
                for k, v in kat_stats.items()
            )

            html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<style>
body{{font-family:Arial,sans-serif;background:#f4f4f4;padding:20px;}}
.box{{max-width:700px;margin:0 auto;background:white;padding:30px;border-radius:10px;}}
h1{{color:#667eea;border-bottom:3px solid #667eea;padding-bottom:10px;}}
.stat{{display:inline-block;background:#f8f9fa;padding:20px;margin:8px;border-radius:8px;text-align:center;min-width:120px;}}
.num{{font-size:32px;font-weight:bold;color:#667eea;}}
table{{width:100%;border-collapse:collapse;margin:15px 0;}}
th{{background:#667eea;color:white;padding:10px;text-align:left;}}
</style></head><body><div class="box">
<h1>NovaBot Gunluk Rapor</h1>
<p><strong>Tarih:</strong> {today}</p>
<div style="text-align:center;margin:20px 0;">
<div class="stat"><div class="num">{stats.get('today',0)}</div><div>Yayinlanan</div></div>
<div class="stat"><div class="num">{stats.get('pending',0)}</div><div>Bekleyen</div></div>
<div class="stat"><div class="num">{stats.get('failed',0)}</div><div>Basarisiz</div></div>
</div>
<h2 style="color:#667eea;">Kategori Durumu</h2>
<table><thead><tr><th>Kategori</th><th>Yayinlanan/Limit</th><th>Kalan</th></tr></thead>
<tbody>{kat_rows}</tbody></table>
<p style="text-align:center;color:#999;font-size:12px;margin-top:20px;">NovaBot v3.0</p>
</div></body></html>"""

            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"NovaBot Gunluk Rapor - {today}"
            msg['From'] = self.smtp_kullanici
            msg['To'] = self.email
            msg.attach(MIMEText(html, 'html', 'utf-8'))

            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_kullanici, self.smtp_sifre)
                server.send_message(msg)

            self.logger.info(f"Rapor gonderildi: {self.email}")
            return True

        except Exception as e:
            self.logger.error(f"Email hatasi: {e}")
            return False


# ════════════════════════════════════════════════════════════════
# 8. WEB PANEL
# ════════════════════════════════════════════════════════════════

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "novabot_secret")
_queue_ref = None
_login_attempts = {}

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect('/login')
        return f(*args, **kwargs)
    return decorated

LOGIN_HTML = """<!DOCTYPE html><html lang="tr"><head><meta charset="UTF-8">
<title>NovaBot - Giris</title>
<style>
*{margin:0;padding:0;box-sizing:border-box;}
body{font-family:'Segoe UI',sans-serif;background:linear-gradient(135deg,#667eea,#764ba2);min-height:100vh;display:flex;align-items:center;justify-content:center;}
.box{background:white;padding:40px;border-radius:15px;box-shadow:0 10px 30px rgba(0,0,0,.3);width:340px;}
h1{color:#667eea;text-align:center;margin-bottom:25px;font-size:22px;}
input{width:100%;padding:14px;border:2px solid #eee;border-radius:8px;margin-bottom:15px;font-size:15px;}
button{width:100%;padding:14px;background:#667eea;color:white;border:none;border-radius:8px;font-size:15px;cursor:pointer;}
.err{color:#e74c3c;text-align:center;margin-bottom:12px;font-size:14px;}
</style></head><body>
<div class="box">
<h1>NovaBot v3.0</h1>
{% if error %}<div class="err">{{ error }}</div>{% endif %}
<form method="POST">
<input type="password" name="password" placeholder="Sifre" required>
<button type="submit">Giris Yap</button>
</form></div></body></html>"""

DASH_HTML = """<!DOCTYPE html><html lang="tr"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>NovaBot - Panel</title>
<style>
*{margin:0;padding:0;box-sizing:border-box;}
body{font-family:'Segoe UI',sans-serif;background:linear-gradient(135deg,#667eea,#764ba2);min-height:100vh;padding:20px;}
.wrap{max-width:1200px;margin:0 auto;}
.header{background:white;padding:20px 30px;border-radius:15px;margin-bottom:25px;display:flex;justify-content:space-between;align-items:center;}
.header h1{color:#667eea;font-size:22px;}
.logout{background:#e74c3c;color:white;padding:10px 20px;border-radius:8px;text-decoration:none;font-size:14px;}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:20px;margin-bottom:25px;}
.card{background:white;padding:25px;border-radius:15px;}
.card h3{color:#888;font-size:12px;text-transform:uppercase;margin-bottom:8px;}
.card .num{font-size:36px;font-weight:bold;color:#667eea;}
.tcard{background:white;padding:25px;border-radius:15px;margin-bottom:20px;overflow-x:auto;}
table{width:100%;border-collapse:collapse;}
th,td{padding:11px;text-align:left;border-bottom:1px solid #eee;}
th{background:#f8f9fa;color:#667eea;font-weight:600;font-size:13px;}
.badge{padding:4px 10px;border-radius:20px;font-size:11px;font-weight:bold;}
.pending{background:#ffeaa7;color:#d63031;}
.published{background:#55efc4;color:#00b894;}
.failed{background:#ff7675;color:#c0392b;}
.publishing{background:#74b9ff;color:#0984e3;}
.progress{background:#eee;border-radius:10px;height:8px;}
.bar{background:#667eea;border-radius:10px;height:8px;}
</style></head><body>
<div class="wrap">
<div class="header">
<h1>NovaBot v3.0 - Kontrol Paneli</h1>
<a href="/logout" class="logout">Cikis Yap</a>
</div>
<div class="grid">
<div class="card"><h3>Bekleyen</h3><div class="num">{{ s.pending }}</div></div>
<div class="card"><h3>Bugun</h3><div class="num">{{ s.today }}</div></div>
<div class="card"><h3>Toplam</h3><div class="num">{{ s.published }}</div></div>
<div class="card"><h3>Basarisiz</h3><div class="num">{{ s.failed }}</div></div>
</div>
<div class="tcard">
<h2 style="color:#667eea;margin-bottom:15px;">Kategori Durumu</h2>
<table><thead><tr><th>Kategori</th><th>Durum</th><th>Ilerleme</th><th>Kalan</th></tr></thead><tbody>
{% for k,v in ks.items() %}
<tr>
<td>{{ k }}</td>
<td>{{ v.yayinlanan }}/{{ v.limit }}</td>
<td style="width:180px;"><div class="progress"><div class="bar" style="width:{{ [v.yayinlanan/v.limit*100,100]|min if v.limit>0 else 0 }}%"></div></div></td>
<td>{{ v.kalan }}</td>
</tr>{% endfor %}
</tbody></table></div>
<div class="tcard">
<h2 style="color:#667eea;margin-bottom:15px;">Son 20 Video</h2>
<table><thead><tr><th>Baslik</th><th>Kategori</th><th>MB</th><th>Durum</th><th>Tarih</th></tr></thead><tbody>
{% for v in videos %}
<tr>
<td>{{ v.baslik[:50] }}...</td>
<td>{{ v.kategori_adi }}</td>
<td>{{ v.boyut_mb }}</td>
<td><span class="badge {{ v.status }}">{{ v.status }}</span></td>
<td>{{ v.created_at[:16] }}</td>
</tr>{% endfor %}
</tbody></table></div>
</div>
<script>setTimeout(()=>location.reload(),30000);</script>
</body></html>"""

@app.route('/login', methods=['GET', 'POST'])
def login():
    panel_pass = os.getenv("WEB_PANEL_PASSWORD")
    if request.method == 'POST':
        ip = request.remote_addr
        attempts = _login_attempts.get(ip, 0)
        if attempts >= 5:
            return render_template_string(LOGIN_HTML, error="Cok fazla deneme!")
        password = request.form.get('password', '')
        if password == panel_pass:
            session['logged_in'] = True
            _login_attempts.pop(ip, None)
            return redirect('/')
        _login_attempts[ip] = attempts + 1
        return render_template_string(LOGIN_HTML, error="Yanlis sifre!")
    return render_template_string(LOGIN_HTML)

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    return redirect('/login')

@app.route('/')
@login_required
def dashboard():
    global _queue_ref
    try:
        conn = sqlite3.connect("novabot.db", timeout=10)
        c = conn.cursor()
        c.execute("SELECT baslik, kategori_adi, boyut_mb, status, created_at FROM queue ORDER BY created_at DESC LIMIT 20")
        videos = [{'baslik': r[0], 'kategori_adi': r[1], 'boyut_mb': r[2], 'status': r[3], 'created_at': r[4]}
                  for r in c.fetchall()]
        conn.close()
        stats = _queue_ref.get_stats() if _queue_ref else {}
        ks = _queue_ref.get_kategori_stats() if _queue_ref else {}
        return render_template_string(DASH_HTML, s=stats, ks=ks, videos=videos)
    except Exception as e:
        return f"Hata: {e}", 500

@app.route('/health')
def health():
    return jsonify({"status": "ok", "time": datetime.now().isoformat()})

@app.route('/api/stats')
@login_required
def api_stats():
    global _queue_ref
    if _queue_ref:
        return jsonify(_queue_ref.get_stats())
    return jsonify({"error": "queue yok"}), 500

def start_panel(queue_manager):
    global _queue_ref
    _queue_ref = queue_manager
    port = int(os.getenv("WEB_PANEL_PORT", 5000))
    app.run(host='127.0.0.1', port=port, debug=False)


# ════════════════════════════════════════════════════════════════
# 9. NOVABOT ANA SINIF
# ════════════════════════════════════════════════════════════════

class NovaBot:
    def __init__(self, logger):
        self.logger = logger
        self.queue = QueueManager(logger)
        self.telegram = TelegramClientWrapper(logger)
        self.wordpress = WordPressClient(logger)
        self.email_reporter = EmailReporter(logger, self.queue)
        self.interval = int(os.getenv("ARALIK", 30))
        self.running = True

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

    def _shutdown(self, signum, frame):
        self.logger.info(f"Sinyal alindi ({signum}), kapatiliyor...")
        self.running = False

    def fetch_videos(self):
        try:
            self.logger.info("Telegram kontrol ediliyor...")
            videos = self.telegram.get_new_videos()
            if not videos:
                self.logger.info("Yeni video yok")
                return
            added = 0
            for video in videos:
                if self.queue.add_video(video):
                    added += 1
                    self.logger.info(f"Queue: {video['baslik']}")
            self.logger.info(f"{added} video eklendi")
        except Exception as e:
            self.logger.exception(f"fetch_videos hatasi: {e}")

    def publish_next(self):
        try:
            video = self.queue.get_next_video()
            if not video:
                self.logger.info("Yayinlanacak video yok")
                return

            self.logger.info(f"Yayinlaniyor: {video['baslik']}")
            success = self.wordpress.publish_video(video)

            if success:
                self.queue.mark_published(video['id'])
                self.logger.info("Yayinlama basarili")
            else:
                self.queue.mark_failed(video['id'])
                self.logger.error("Yayinlama basarisiz")

        except Exception as e:
            self.logger.exception(f"publish_next hatasi: {e}")

    def run_cycle(self):
        self.logger.info("=" * 50)
        self.logger.info("DONGU BASLADI")
        try:
            self.fetch_videos()
            self.publish_next()
            stats = self.queue.get_stats()
            self.logger.info(f"Bugun: {stats.get('today', 0)} yayinlandi")
        except Exception as e:
            self.logger.exception(f"run_cycle hatasi: {e}")
        finally:
            self.logger.info("DONGU BITTI")
            self.logger.info("=" * 50)

    def start(self):
        self.logger.info("=" * 50)
        self.logger.info("NOVABOT v3.0 BASLIYOR")
        self.logger.info(f"Interval: {self.interval} dakika")
        self.logger.info(f"Site: {os.getenv('WP_URL')}")
        self.logger.info("=" * 50)

        if os.getenv("ENABLE_WEB_PANEL", "true").lower() == "true":
            try:
                t = threading.Thread(target=start_panel, args=(self.queue,), daemon=True)
                t.start()
                self.logger.info("Web panel baslatildi (127.0.0.1:5000)")
            except Exception as e:
                self.logger.warning(f"Web panel hatasi: {e}")

        schedule.every().sunday.at("03:00").do(self.queue.cleanup_old_records)
        schedule.every().day.at("23:00").do(self.email_reporter.send_daily_report)

        self.run_cycle()
        schedule.every(self.interval).minutes.do(self.run_cycle)

        while self.running:
            try:
                schedule.run_pending()
                time.sleep(60)
            except Exception as e:
                self.logger.exception(f"Ana dongu hatasi: {e}")
                time.sleep(60)

        self.logger.info("Bot durduruldu")

    def status(self):
        stats = self.queue.get_stats()
        ks = self.queue.get_kategori_stats()
        print("\n" + "=" * 60)
        print("NOVABOT v3.0 - DURUM")
        print("=" * 60)
        print(f"Pending    : {stats.get('pending', 0)}")
        print(f"Publishing : {stats.get('publishing', 0)}")
        print(f"Bugun      : {stats.get('today', 0)}")
        print(f"Toplam     : {stats.get('published', 0)}")
        print(f"Basarisiz  : {stats.get('failed', 0)}")
        print("-" * 60)
        print("Kategori:")
        for k, v in ks.items():
            print(f"  {k}: {v['yayinlanan']}/{v['limit']} (Kalan: {v['kalan']})")
        print("=" * 60 + "\n")


# ════════════════════════════════════════════════════════════════
# 10. SESSION CREATOR
# ════════════════════════════════════════════════════════════════

async def create_session():
    print("\n" + "=" * 60)
    print("TELEGRAM SESSION OLUSTURULUYOR")
    print("=" * 60)
    client = TelethonClient(
        'novabot_session',
        os.getenv("TELEGRAM_API_ID"),
        os.getenv("TELEGRAM_API_HASH")
    )
    await client.start(phone=os.getenv("TELEGRAM_PHONE"))
    print("Session olusturuldu: novabot_session.session")
    print("=" * 60 + "\n")
    await client.disconnect()


# ════════════════════════════════════════════════════════════════
# 11. MAIN
# ════════════════════════════════════════════════════════════════

def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--create-session":
        load_dotenv("config.env")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(create_session())
        loop.close()
        sys.exit(0)

    logger = setup_logging()
    validate_config(logger)
    bot = NovaBot(logger)

    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "status":
            bot.status()
        elif cmd == "force_publish":
            bot.publish_next()
        else:
            print("Komutlar: status | force_publish | --create-session")
        sys.exit(0)

    bot.start()


if __name__ == "__main__":
    main()
