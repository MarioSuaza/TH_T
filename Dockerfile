# =============================================================================
# Imagen unica para el pipeline, las pruebas y el tablero.
# =============================================================================
# Se usa una sola imagen en lugar de tres porque las tres necesitan exactamente
# las mismas dependencias: mantener imagenes separadas anadiria superficie de
# mantenimiento sin ganar nada.
# =============================================================================
FROM python:3.11-slim-bookworm

# La version menor de Python queda fijada. El tag de la imagen base puede
# recibir parches upstream; para una build byte a byte habria que fijar tambien
# su digest y usar un lock de dependencias con hashes.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=UTC \
    PYTHONPATH=/app \
    TYBA_PROJECT_ROOT=/app

WORKDIR /app

# Las dependencias van en una capa propia para que un cambio en el codigo no
# invalide la cache de pip.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Usuario no root. Se crea antes de copiar para poder asignar la propiedad en
# un solo paso.
RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin tyba

COPY --chown=tyba:tyba config/    /app/config/
COPY --chown=tyba:tyba sql/       /app/sql/
COPY --chown=tyba:tyba src/       /app/src/
COPY --chown=tyba:tyba scripts/   /app/scripts/
COPY --chown=tyba:tyba dashboard/ /app/dashboard/
COPY --chown=tyba:tyba tests/     /app/tests/
COPY --chown=tyba:tyba pytest.ini /app/

# Los directorios de datos se crean con el propietario correcto: si el volumen
# se monta vacio, el proceso no root puede escribir en el.
RUN mkdir -p /app/data/raw /app/data/reports /app/data/database \
    && chown -R tyba:tyba /app/data

USER tyba

# Comprueba que el interprete, la configuracion y el motor responden.
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import duckdb; from src.config import load_config; load_config()" || exit 1

ENTRYPOINT ["python", "-m"]
CMD ["src.pipeline", "--input-directory", "/app/data/raw"]
