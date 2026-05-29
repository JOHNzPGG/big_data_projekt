import json
import time
import requests
import ipaddress
import urllib3
from datetime import datetime
from collections import defaultdict
from confluent_kafka import Producer

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

WIKI_URL    = "https://stream.wikimedia.org/v2/stream/recentchange"
KAFKA_TOPIC = "wiki-edits"

# ─────────────────────────────────────────────
#  GEOLOKALIZACJA — BUFFER-THEN-SEND
#
#  Poprzednia architektura (ZEPSUTA):
#    event → Kafka(lat=null) → [tło: geo cache]
#    Problem: to samo IP prawie nigdy nie wraca,
#    więc cache nigdy nie jest używany → mapa pusta.
#
#  Nowa architektura (POPRAWIONA):
#    event (IP) → bufor → [batch geo API] → Kafka(lat,lon)
#    event (login) → Kafka natychmiast (brak geo)
#
#  Efekt: krótkie opóźnienie (~3s) dla IP-ów,
#  ale KAŻDY event z IP ma wypełnione współrzędne.
# ─────────────────────────────────────────────

_geo_cache: dict   = {}    # ip → (lat, lon), tylko sukcesy
_ip_buffer: list   = []    # lista dict-ów czekających na geo
_last_flush: float = 0.0

BUFFER_MAX     = 30    # flush po N eventach z IP
BUFFER_TIMEOUT = 3.0   # lub po N sekundach (cokolwiek pierwsze)


def _is_ip(s: str) -> bool:
    try:
        ipaddress.ip_address(s)
        return True
    except ValueError:
        return False


def _batch_resolve(ips: list) -> dict:
    """
    Wywołuje ip-api.com/batch. Max 100 IP na zapytanie, limit 15 req/min
    (ale każde zapytanie = 100 IP, więc efektywnie 1500 IP/min).
    Zwraca {ip: (lat, lon)} tylko dla sukcesów.
    """
    result = {}
    if not ips:
        return result

    # Dzielimy na paczki po 100 (limit API)
    for i in range(0, len(ips), 100):
        chunk = ips[i:i+100]
        try:
            payload = [{"query": ip, "fields": "status,lat,lon,query"} for ip in chunk]
            r = requests.post(
                "http://ip-api.com/batch",
                json=payload,
                timeout=6,
                headers={"User-Agent": "WikiEditWarsMonitor/2.0"},
            )
            for item in r.json():
                ip = item.get("query", "")
                if ip and item.get("status") == "success":
                    result[ip] = (item["lat"], item["lon"])
                    print(f"  📍 {ip} → {item['lat']:.2f}, {item['lon']:.2f}")
        except Exception as exc:
            print(f"  [GEO WARN] batch failed: {exc}")

    return result


def flush_ip_buffer(kafka_prod):
    """
    Rozwiązuje geo dla wszystkich zbuforowanych IP-ów,
    uzupełnia lat/lon w eventach i wysyła do Kafki.
    """
    global _last_flush

    if not _ip_buffer:
        _last_flush = time.time()
        return

    batch = _ip_buffer.copy()
    _ip_buffer.clear()
    _last_flush = time.time()

    # Zbieramy IP których jeszcze nie ma w cache
    uncached = list({ev["user"] for ev in batch if ev["user"] not in _geo_cache})

    if uncached:
        new_geo = _batch_resolve(uncached)
        _geo_cache.update(new_geo)

    # Uzupełniamy lat/lon i wysyłamy
    resolved = 0
    for ev in batch:
        ip = ev["user"]
        if ip in _geo_cache:
            ev["lat"], ev["lon"] = _geo_cache[ip]
            resolved += 1
        kafka_prod.produce(
            KAFKA_TOPIC,
            value=json.dumps(ev, ensure_ascii=False).encode("utf-8"),
        )
    kafka_prod.poll(0)

    print(f"  🗺 Flush {len(batch)} IP-eventów → {resolved} z geo")


def _needs_flush() -> bool:
    return (len(_ip_buffer) >= BUFFER_MAX or
            (bool(_ip_buffer) and time.time() - _last_flush >= BUFFER_TIMEOUT))


# ─────────────────────────────────────────────
#  DETEKCJA UKRYTYCH BOTÓW
# ─────────────────────────────────────────────
_user_edit_times: dict = defaultdict(list)
BOT_EDITS_PER_MIN = 10

def is_suspicious_bot(user: str, ts: float) -> bool:
    times  = _user_edit_times[user]
    cutoff = ts - 60.0
    times  = [t for t in times if t > cutoff]
    times.append(ts)
    _user_edit_times[user] = times
    return len(times) >= BOT_EDITS_PER_MIN


# ─────────────────────────────────────────────
#  KAFKA
# ─────────────────────────────────────────────
kafka_producer = Producer({"bootstrap.servers": "localhost:9092"})

def _delivery_cb(err, msg):
    if err:
        print(f"[Kafka ERR] {err}")


# ─────────────────────────────────────────────
#  JEDNA PRÓBA POŁĄCZENIA SSE
# ─────────────────────────────────────────────
def _stream_once(session: requests.Session):
    resp = session.get(
        WIKI_URL,
        stream=True,
        headers={"User-Agent": "WikiEditWarsMonitor/2.0 (student project)"},
        timeout=(10, 30),
    )
    resp.raise_for_status()
    print("✓ Połączono z Wikimedia SSE. Przesyłam do Kafki…\n")

    for raw_line in resp.iter_lines():
        if not raw_line:
            continue
        line = raw_line.decode("utf-8")
        if not line.startswith("data: "):
            continue

        try:
            data = json.loads(line[6:])
        except json.JSONDecodeError:
            continue

        if data.get("type") != "edit":
            continue

        try:
            user:     str  = data["user"]
            is_bot:   bool = data.get("bot", False)
            now:      float = time.time()
            suspicious     = (not is_bot) and is_suspicious_bot(user, now)

            server: str = data.get("server_name", "")
            lang = server.split(".")[0] if "." in server else "??"

            ev = {
                "time":          datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "user":          user,
                "is_bot":        is_bot,
                "is_suspicious": suspicious,
                "title":         data["title"],
                "wiki_lang":     lang,
                "length_diff":   (data.get("length", {}).get("new", 0)
                                  - data.get("length", {}).get("old", 0)),
                "comment":       data.get("comment", ""),
                "revision_id":   data.get("revision", {}).get("new", 0),
                "is_minor":      data.get("minor", False),
                "lat":           None,
                "lon":           None,
            }

            if _is_ip(user):
                # IP: sprawdź cache, jeśli brak → bufor
                if user in _geo_cache:
                    ev["lat"], ev["lon"] = _geo_cache[user]
                    kafka_producer.produce(
                        KAFKA_TOPIC,
                        value=json.dumps(ev, ensure_ascii=False).encode("utf-8"),
                        callback=_delivery_cb,
                    )
                    kafka_producer.poll(0)
                    print(f"📍 [{lang}] {ev['lat']:.1f},{ev['lon']:.1f}  {data['title'][:45]}")
                else:
                    _ip_buffer.append(ev)
                    print(f"⏳ [{lang}] (bufor {len(_ip_buffer)})  {data['title'][:45]}")
            else:
                # Zalogowany użytkownik: wyślij od razu
                kafka_producer.produce(
                    KAFKA_TOPIC,
                    value=json.dumps(ev, ensure_ascii=False).encode("utf-8"),
                    callback=_delivery_cb,
                )
                kafka_producer.poll(0)
                flag = "🤖?" if suspicious else ("🤖" if is_bot else "👤")
                print(f"{flag} [{lang}]  {data['title'][:50]}")

        except KeyError:
            continue

        # Flush bufora jeśli czas lub rozmiar przekroczony
        if _needs_flush():
            flush_ip_buffer(kafka_producer)


# ─────────────────────────────────────────────
#  GŁÓWNA PĘTLA — AUTO-RECONNECT + BACKOFF
# ─────────────────────────────────────────────
def start_server():
    retry_delay = 5
    max_delay   = 120
    attempt     = 0
    http        = requests.Session()
    http.verify = False

    while True:
        attempt += 1
        print(f"\n── Próba #{attempt} ─────────────────────────────")
        try:
            _stream_once(http)
            print("Strumień zakończył się normalnie. Ponawiam…")
            retry_delay = 5

        except KeyboardInterrupt:
            print("\nZatrzymuję (Ctrl+C)…")
            flush_ip_buffer(kafka_producer)   # wyślij co zostało w buforze
            break

        except requests.exceptions.HTTPError as exc:
            code = exc.response.status_code
            print(f"[HTTP {code}] {exc}")
            if code in (401, 403):
                print("Brak autoryzacji — kończę.")
                break

        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ReadTimeout,
            requests.exceptions.Timeout,
        ) as exc:
            print(f"[POŁĄCZENIE] {type(exc).__name__}: {exc}")

        except Exception as exc:
            print(f"[BŁĄD] {type(exc).__name__}: {exc}")

        # Przed reconnectem flush bufor (żeby nie zgubić zebranych IP-ów)
        if _ip_buffer:
            print(f"  Flushuję {len(_ip_buffer)} oczekujących eventów przed reconnectem…")
            flush_ip_buffer(kafka_producer)

        print(f"Ponawiam za {retry_delay}s…")
        time.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, max_delay)

    kafka_producer.flush()
    print("Producent zakończył pracę.")


if __name__ == "__main__":
    start_server()
