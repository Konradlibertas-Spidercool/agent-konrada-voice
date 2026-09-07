# Agent Konrada — serwis głosowy

Serwis przesyła dźwięk między Twilio i OpenAI Realtime. Panel, sprawy, limity upoważnień i ustalenia pozostają w aplikacji Sites.

## Konfiguracja Render

- Typ: Web Service
- Repozytorium: agent-konrada-voice
- Branch: main
- Language/Runtime: Docker
- Region: Frankfurt
- Root Directory: puste (pliki są w głównym katalogu)
- Dockerfile Path: ./Dockerfile
- Health Check Path: /health
- Jedna stale działająca instancja; cenę wybranego planu zatwierdza właściciel.
- Wyłącz automatyczne wdrożenia podczas testów telefonicznych: restart może przerwać rozmowę.

Sekrety wpisuje się wyłącznie w Environment w Renderze:
OPENAI_API_KEY, TWILIO_AUTH_TOKEN, VOICE_BRIDGE_SECRET.

Pozostałe ustawienia:
OPENAI_REALTIME_MODEL=gpt-realtime-2.1
SITE_URL=https://agent-konrada.konradlibertas.chatgpt.site
PUBLIC_BASE_URL=https://adres-serwisu-na-render

VOICE_BRIDGE_SECRET musi być identyczny w Renderze i w Sites. Po sprawdzeniu serwisu i podpisanego WebSocket należy ustawić w Sites VOICE_BRIDGE_URL na jego adres HTTPS. Do tego czasu panel korzysta z dotychczasowego trybu HTTP.

Kod nie zawiera kluczy dostępowych. /health zwraca gotowość konfiguracji, nie potwierdza jeszcze poprawności kluczy ani całej rozmowy. Przed włączeniem trzeba sprawdzić odbiór głosu w obie strony, przerwania, zakończenie po odtworzeniu pożegnania i zapis ustaleń.
