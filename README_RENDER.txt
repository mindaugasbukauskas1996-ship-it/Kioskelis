Deploy i Render:

1. Ikelk si projekta i GitHub (be venv katalogo).
2. Render -> New -> Web Service.
3. Prijunk GitHub repo.
4. Build Command:
   pip install -r requirements.txt && python -m playwright install --with-deps chromium
5. Start Command:
   python app.py
6. Environment Variables:
   SYN_USER=... tavo el. pastas / user
   SYN_PASS=... tavo slaptazodis
   POLL_SECONDS=60
   DEBUG=0
   REPORT_PROJECT_DIV_ID=5720
   REPORT_PROJECT_TEXT=Mano Būstas rezultatai nuo
   RUN_WORKER=1

Atidaryk gauta Render URL. Pagrindinis puslapis:
/
Duomenys JSON:
/data.json
Sveikatos patikra:
/healthz

Pastaba:
- Pirmas uzkrovimas gali uztrukti, kol Playwright prisijungs ir sugeneruos public/index.html.
- Render free plane paslauga gali uzmigti, jei kurį laiką niekas neatidaro puslapio.
