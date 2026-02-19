# SteamPulse — Steam Price Analytics Dashboard

## Estructura del proyecto

```
steamproyect/
├── steam_price_history.py   # Recolector de datos (ya tienes este)
├── histograms/              # Parquets generados
├── build_db.py              # Convierte parquets → DuckDB
├── api.py                   # Backend FastAPI
├── index.html               # Dashboard web
└── steam.db                 # Base de datos (se genera con build_db.py)
```

---

## Paso 1 — Convertir parquets a DuckDB

```bash
pip install duckdb pandas pyarrow
python build_db.py --parquet-dir histograms --db steam.db
```

Verás algo como:
```
INFO Tabla creada: 850 registros, 187 juegos, años 2022-2025
INFO Base de datos guardada en steam.db (0.8 MB)
```

---

## Paso 2 — Levantar el backend

```bash
pip install fastapi uvicorn
uvicorn api:app --reload
```

Prueba que funciona:
- http://localhost:8000/summary
- http://localhost:8000/games
- http://localhost:8000/games/730/history

---

## Paso 3 — Abrir el dashboard

Simplemente abre `index.html` en tu navegador.
El dashboard se conecta automáticamente al backend en localhost:8000.

> Si la API no está corriendo, el dashboard entra en **modo demo** automáticamente.

---

## Deploy gratuito en Render.com

### Backend (FastAPI)

1. Sube el proyecto a GitHub
2. En render.com → New Web Service
3. Conecta tu repo
4. Configura:
   - Build command: `pip install fastapi uvicorn duckdb pandas pyarrow`
   - Start command: `uvicorn api:app --host 0.0.0.0 --port $PORT`
5. Agrega variable de entorno: `STEAM_DB=steam.db`
6. Sube el archivo `steam.db` al repo (si es pequeño) o usa un disco persistente

### Frontend

Cambia en `index.html` la línea:
```js
const API = "http://localhost:8000";
```
por la URL de tu servicio en Render:
```js
const API = "https://tu-app.onrender.com";
```

Luego sube `index.html` a Vercel, Netlify, o GitHub Pages (gratis).

---

## Para el proyecto final (5,000+ juegos, 5+ años)

El script acepta:
```bash
python steam_price_history.py \
  --itad-key TU_KEY \
  --top-n 5000 \
  --since 2019-01-01T00:00:00Z \
  --skip-game-check
```

Tiempo estimado: ~3-4 horas (respetando rate limits de ITAD).
El archivo `steam.db` resultante será de aproximadamente 50-200 MB.
