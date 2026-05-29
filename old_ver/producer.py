import json
import requests
from datetime import datetime
import urllib3
from confluent_kafka import Producer
import os
import urllib.request
import maxminddb
import ipaddress

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

WIKI_URL = 'https://stream.wikimedia.org/v2/stream/recentchange'
KAFKA_TOPIC = 'wiki-edits'

# --- GEOLOKALIZACJA SETUP ---
DB_FILE = "GeoLite2-City.mmdb"
if not os.path.exists(DB_FILE):
    print("Pobieranie darmowej bazy geolokalizacyjnej (ok. 30MB)...")
    try:
        urllib.request.urlretrieve("https://raw.githubusercontent.com/P1sec/GeoLite2-City/master/GeoLite2-City.mmdb", DB_FILE)
        print("Baza map pobrana pomyślnie!")
    except Exception as e:
        print(f"Błąd pobierania bazy: {e}")

# Otwieramy bazę do szybkiego czytania
try:
    geo_reader = maxminddb.open_database(DB_FILE)
except:
    geo_reader = None

def get_lat_lon(user_string):
    if not geo_reader: return None, None
    try:
        ipaddress.ip_address(user_string) # Sprawdza czy tekst to poprawny adres IP
        geo = geo_reader.get(user_string)
        if geo and 'location' in geo:
            return geo['location']['latitude'], geo['location']['longitude']
    except ValueError:
        pass # To jest zwykły login człowieka (np. JanKowalski)
    return None, None
# -----------------------------

# Konfiguracja Producenta Kafki
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"Błąd dostarczenia wiadomości: {err}")


def start_server():
    print(f"Łączenie z URL: {WIKI_URL} ...")
    try:
        headers = {'User-Agent': 'SparkStudentProject/1.0'}
        response = requests.get(WIKI_URL, stream=True, verify=False, headers=headers, timeout=10)

        if response.status_code != 200:
            print(f"Błąd! Serwer zwrócił kod: {response.status_code}")
            return

        print("Połączenie udane! Przesyłam dane do Kafki...")

        for line in response.iter_lines():
            if not line:
                continue
            decoded_line = line.decode('utf-8')

            if decoded_line.startswith("data: "):
                try:
                    json_content = decoded_line.replace("data: ", "")
                    data = json.loads(json_content)

                    if data['type'] == 'edit':
                        lat, lon = get_lat_lon(data['user'])

                        spark_data = {
                            "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            "user": data['user'],
                            "is_bot": data['bot'],
                            "title": data['title'],
                            "length_diff": data.get('length', {}).get('new', 0) - data.get('length', {}).get('old', 0),
                            "comment": data.get('comment', ''),
                            "revision_id": data.get('revision', {}).get('new', 0),
                            "is_minor": data.get('minor', False),
                            "lat": lat,  # <-- DODANO
                            "lon": lon  # <-- DODANO
                        }

                        # WYSYŁKA DO KAFKI
                        producer.produce(
                            KAFKA_TOPIC,
                            value=json.dumps(spark_data).encode('utf-8'),
                            callback=delivery_report
                        )
                        producer.poll(0)  # Asynchroniczna obsługa zdarzeń
                        print(f"Wysłano do Kafki: {spark_data['title'][:30]}...")

                except json.JSONDecodeError:
                    pass
                except KeyError:
                    pass  # Pomijamy rekordy o nietypowej strukturze

    except Exception as e:
        print(f"\nBŁĄD KRYTYCZNY SIECI: {e}")
    finally:
        producer.flush()
        print("Zakończono przesyłanie.")


if __name__ == "__main__":
    start_server()