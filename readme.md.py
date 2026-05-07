Wikipedia Live Edit Wars

To rozproszony system Big Data, który na żywo nasłuchuje wszystkich edycji na Wikipedii (z całego świata, w każdym języku). Jego głównym zadaniem jest wykrywanie tzw. "wojen edycyjnych" – sytuacji, w których w bardzo krótkim czasie kilku użytkowników (lub botów) naprzemiennie edytuje lub cofa swoje zmiany w tym samym artykule.

Jak to działa pod maską (Stos technologiczny)?
System składa się z trzech niezależnych modułów (mikroserwisów), które gadają ze sobą w czasie rzeczywistym:

1. Ingestion (Producent - Python + API):
Skrypt łączy się ze światowym strumieniem Wikimedia EventStreams API (Server-Sent Events). Odbiera surowe paczki JSON tysięcy edycji na sekundę, wyciąga z nich najważniejsze dane (kto, co, ile znaków dodano/usunięto, z jakim komentarzem) i wysyła je na kolejkę.

2. Bufor (Apache Kafka w Dockerze):
Pomiędzy pobieraniem a analizą stoi Kafka. Pełni rolę niezawodnego bufora pamięci. Dzięki niej, nawet jeśli padnie serwer analityczny, nie tracimy ani jednego zdarzenia z Wikipedii.

3. Mózg Analityczny (Apache Spark Structured Streaming):
Silnik Big Data nasłuchuje strumienia z Kafki. Używa mechanizmu Sliding Windows (przesuwających się okien czasowych). Grupuje dane w 5-minutowe bloki. Jeśli w ciągu 5 minut dany artykuł ma więcej niż X edycji od minimum 2 różnych użytkowników, Spark odpala alert. Skleja też w jeden tekst pseudonimy walczących oraz powód ostatniej edycji i zrzuca to jako "Zdarzenie" do bazy (plików CSV).

4. Interfejs i Analityka (Streamlit + Pandas):
Aplikacja webowa działająca na żywo. Co 10 sekund agreguje dane wyplute przez Sparka. Używa Pandas do zaawansowanej deduplikacji (usuwa powtórki wynikające z nakładających się na siebie okien czasowych, pokazując tylko "szczyt" wojny).

Główne ficzery na ekranie (Dashboard):

Sortowanie: Pozwala filtrować konflikty (np. z ostatnich 15 minut, godziny lub całej sesji).

KPI i Trend: Live-wykres pokazujący natężenie ruchu i kłótni na serwerach w postaci fali.

Moduł Detektywistyczny: Generuje specjalne, dynamiczne linki (diff), w które wystarczy kliknąć, by zobaczyć na własne oczy dwukolumnowe porównanie w kodzie HTML – czyli co dokładnie dany bot lub użytkownik usunął, a co dopisał.