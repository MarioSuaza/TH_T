# Decisiones técnicas (ADR)

Formato: Decisión · Contexto · Alternativas · Elección · Justificación · Consecuencias.

---

## ADR-001 · DuckDB como motor de persistencia y de cómputo

**Contexto.** El enunciado permite DuckDB o PostgreSQL. El problema es un batch
diario, mono-escritor, cuya operación central es un `FULL OUTER JOIN` entre el
corte entrante y el estado vigente. Los datos llegan en Parquet. Debe correr con
`docker compose up --build` sin configuración adicional y escalar a millones de
filas.

**Alternativas.**

| Opción | Por qué se descartó |
| --- | --- |
| **PostgreSQL** | Añade un servicio, un healthcheck, un volumen y una espera de arranque al compose. Su ventaja real es la concurrencia multi-escritor, que aquí no existe: un batch diario tiene un único escritor. Además habría que cargar el Parquet a tablas antes de poder consultarlo, un paso que con DuckDB no existe. |
| **Spark / Databricks** | Sobreingeniería evidente para 50.000 filas y también para 10 millones. El coste de arranque de una sesión de Spark supera el tiempo total del pipeline actual. Se justifica a partir de decenas de miles de millones de filas o cuando el cómputo no cabe en una máquina. |
| **pandas + SQLite** | pandas obliga a materializar el dataset completo en memoria, justo lo que el enunciado advierte que no escala. SQLite no tiene `FULL OUTER JOIN` nativo (hasta 3.39), ni lectura de Parquet, ni DECIMAL real. |
| **Delta Lake / Iceberg sobre Parquet** | Aporta viaje en el tiempo y transacciones sobre un lago. Aquí el histórico se resuelve con SCD2 en tablas, que es más simple de consultar y de auditar. Sería la evolución natural si el volumen creciera mucho. |

**Elección.** DuckDB, archivo único en `data/database/movements.duckdb`.

**Justificación.**

- Lee Parquet directamente con *predicate* y *projection pushdown*: nunca se
  materializa el archivo entero en memoria de Python.
- SQL analítico completo: `FULL OUTER JOIN`, funciones de ventana, `QUALIFY`,
  `list_filter`, `LATERAL`. La primitiva exacta que el problema necesita.
- `DECIMAL` exacto hasta 38 dígitos: sin errores de coma flotante en los importes.
- Transacciones ACID sobre un solo archivo, sin servicio adicional.
- Desbordamiento a disco configurable: procesa conjuntos mayores que la RAM disponible.
- Cero configuración para el evaluador: la base es un archivo que se puede
  copiar, versionar o abrir con la CLI de DuckDB.

**Limitaciones que asume.** Un único proceso escritor a la vez. Sin replicación
ni alta disponibilidad. Sin control de acceso por usuario. El archivo crece
monolíticamente (1,24 GB con 2 millones de filas procesadas, medido).

**Cuándo dejaría de ser suficiente:** concurrencia de escritura, más de ~50 GB
de estado vigente, o necesidad de servir consultas a muchos usuarios
simultáneos.

**Consecuencias.** El compose no tiene dependencias entre servicios ni esperas.
El segundo ciclo Docker limpio procesó los dos archivos entregados en 2,34 s,
incluidos los reportes por corte.

---

## ADR-002 · Clave de negocio derivada + ordinal como identidad del movimiento

**Contexto.** Los archivos no traen el `id` que documenta el glosario. La columna
existente, `id_cliente`, tiene 3.000 valores distintos sobre 50.000 filas y es
idéntica en ambos cortes. Sin una identidad de movimiento no se puede clasificar
nada.

**Alternativas.**

| Opción | Resultado sobre los datos reales | Por qué se descartó |
| --- | --- | --- |
| Usar `id_cliente` como el `id` del enunciado | 0 NEW, 0 DELETED, 3.000 UPDATED | No responde a ninguno de los cuatro casos del enunciado. El grano sería el cliente, no el movimiento. |
| Hash de la fila completa como identidad | 35.000 UNCHANGED, 14.000 NEW, 15.000 DELETED, **0 UPDATED** | Contradice el requisito explícito de detectar "registro corregido": toda corrección se vería como una baja más un alta. |
| Posición de la fila en el archivo | Recupera la alineación exacta | Frágil hasta lo inaceptable: cualquier reordenación del origen rompe toda la identidad. No es una propiedad del dato. |
| Clave de negocio + ordinal | **34.075 UNCHANGED, 9.761 NEW, 3.724 UPDATED, 10.658 DELETED** | Elegida. |

**Elección.**

```
movement_key = SHA-256( "v1" + SEP + id_cliente + SEP + fecha_ISO + SEP + product + SEP + type + SEP + fund + SEP + ordinal )
atributos mutables = amount, description, commercial_name
```

`SEP` es el carácter de control U+001F (separador de unidad).

**Justificación.** Se validó empíricamente antes de implementar: al alinear los
dos cortes y cruzar las filas desaparecidas con las aparecidas usando esta clave
sobre los valores **normalizados**, salen unos 3.900 pares que difieren
únicamente en `amount`, `description` y/o `commercial_name`. Eso es literalmente
la definición de "registro corregido" del enunciado. La partición resultante
cuadra con los totales de ambos archivos. Ya en ejecución, sobre las filas
validadas, el pipeline clasifica **3.724 `UPDATED`**: esa es la cifra
reproducible que conviene citar.

Dato revelador: de esos pares, 1 tenía la fecha escrita en otro formato y 2
tenían el fondo en otras mayúsculas. **Sin normalizar previamente, la identidad
se rompe.** Por eso la normalización va antes del cálculo de la clave.

**Consecuencias.**

- La clave está declarada en `config/data_contract.yml → identity.business_key`.
  Cambiarla es cambiar una lista, no tocar código.
- 116 grupos en T y 105 en T+1 comparten clave. Se desempatan con un ordinal
  determinista ordenado por los atributos mutables y se marcan con
  `is_key_ambiguous`. Riesgo asumido: si el importe de uno de ellos cambia, el
  ordinal puede reasignarse y producir dos correcciones en vez de una. Afecta
  como máximo al 0,45% de las filas y es medible.
- El hash usa serialización estable: orden fijo de campos, separador U+001F
  (verificado ausente en los datos), token explícito de nulo U+001E+`NULL`, fecha
  ISO y decimal de escala fija. Ni el orden de las columnas, ni el orden de las
  filas, ni el formato de origen alteran la identidad. Hay pruebas para los tres
  casos.

---

## ADR-003 · Tres capas ligeras dentro de una sola base

**Contexto.** Bronze / Silver / Gold es la convención habitual, pero el enunciado
no la exige y aplicarla por costumbre podría añadir complejidad sin valor.

**Alternativas.**

| Opción | Valoración |
| --- | --- |
| Una sola capa: leer el Parquet y escribir el estado vigente | Más simple, pero no permite auditar qué se rechazó ni por qué, ni reprocesar sin volver al archivo original. Incompatible con "sin perder trazabilidad". |
| Bronze / Silver / Gold con archivos físicos por capa | Triplica el almacenamiento y añade sincronización entre capas sin ganancia real a este volumen. |
| **Tres capas lógicas dentro de una base, con la zona raw inmutable en disco** | Elegida. |

**Elección.**

- **Bronze**: los `.parquet` originales, intactos, más `file_registry` con hash,
  tamaño, esquema, número de filas y estado de procesamiento.
- **Silver**: `stg_movements` (tipado, normalizado, con valores originales
  conservados y hashes calculados), `rejected_records` y `data_quality_flags`.
- **Gold**: `movements_current`, `movements_history`, `movement_changes`,
  `movement_change_fields` y las vistas analíticas.

**Justificación.** Cada capa responde a una pregunta distinta que el ejercicio
plantea explícitamente: *qué llegó* (Bronze), *qué era utilizable y qué no*
(Silver), *cuál es la verdad hoy y cómo llegamos hasta aquí* (Gold). No es
convención: sin Silver no hay cuarentena auditable, y sin Bronze no hay
idempotencia por hash.

**Consecuencias.** Silver es transitoria: se vacía por `run_id` al inicio de cada
ejecución. No es un almacén, es una zona de trabajo. Eso evita que crezca sin
control.

---

## ADR-004 · SCD tipo 2 con baja lógica

**Contexto.** Hay que conservar el histórico completo sin duplicar el estado
vigente y garantizando como máximo una fila vigente por movimiento.

**Alternativas.**

| Opción | Por qué se descartó |
| --- | --- |
| Solo estado vigente | Pierde el histórico. Incompatible con el enunciado. |
| Solo histórico, con el vigente como vista | Cada consulta del estado actual pagaría un `QUALIFY row_number()`. Es el 90% de las consultas del tablero. |
| Log de eventos puro (event sourcing) | Reconstruir el estado exige replay. Sobreingeniería para un batch diario. |
| **`movements_current` + `movements_history` (SCD2) + bitácora** | Elegida. |

**Elección.** `movements_current` con `PRIMARY KEY (movement_key)` e `is_active`;
`movements_history` con `(movement_key, version)`, `valid_from`, `valid_to`,
`is_current`, `is_deleted`.

**Detalle no obvio.** Una baja **no crea una versión vacía**. Cierra la versión
vigente (`valid_to = fecha del corte`, `is_current = false`, `is_deleted = true`)
y marca `movements_current.is_active = false`. Una fila fantasma sin datos no
aporta nada y ensuciaría las agregaciones sobre el histórico.

**Consecuencias.** La PRIMARY KEY garantiza a nivel de motor la invariante "como
máximo una fila vigente por movimiento": no depende de que el código sea
correcto. Se verifica además con un control de reconciliación y una invariante
estructural en cada ejecución.

---

## ADR-005 · Guardas configurables de dos niveles

**Contexto.** El mayor riesgo operativo del diseño es que un corte truncado se
interprete como miles de bajas legítimas y se destruya el estado.

**Alternativas.**

| Opción | Por qué se descartó |
| --- | --- |
| Sin guardas | Un fallo de extracción borraría el estado sin que nadie se entere. |
| Umbral único | O es tan permisivo que no protege, o tan estricto que bloquea cargas legítimas. |
| Umbral fijo en el código | Convertiría en regla de negocio lo que es una decisión operativa. |
| **Dos niveles (aviso / fallo), configurables, con mínimo de volumen** | Elegida. |

**Elección.** Cuatro guardas: porcentaje de bajas, caída de volumen, variación
monetaria y filas válidas mínimas. Cada una con umbral de aviso y de fallo. Al
superar el de fallo, la ejecución termina con código 3, estado `FAILED_GUARD` y
**sin aplicar ningún cambio**.

**Calibración honesta.** Los valores por defecto se eligieron contra lo observado
entre T y T+1: 21,99% de bajas y 2,0% de caída de volumen. El aviso está en 10%,
así que **sobre los datos reales la alerta se dispara** y queda registrada; el
fallo está en 30% y no se dispara. El mecanismo se demuestra funcionando sin
bloquear la entrega.

**Matiz añadido.** Los umbrales porcentuales solo se evalúan por encima de 100
filas (`guards.min_rows_for_ratio_guards`). Con 3 movimientos vigentes, una baja
es el 33% y ese número no informa de nada. Sin este matiz, cualquier corte
pequeño fallaría por construcción.

**Consecuencias.** Un corte truncado al 2% de su tamaño no aplica ninguna baja:
hay una prueba que lo verifica (`test_una_caida_masiva_de_volumen_no_aplica_las_bajas`).

---

## ADR-006 · `DECIMAL(20,2)` en toda la persistencia monetaria

**Contexto.** El Parquet trae `amount` como `DOUBLE`. Los importes llegan a
5 × 10⁷ y se agregan sobre decenas de miles de filas.

**Alternativas.** Mantener `DOUBLE` (rápido y nativo, pero `0.1 + 0.2 ≠ 0.3` y el
error se acumula en las sumas). Usar enteros de centavos (exacto, pero obliga a
convertir en cada lectura y hace las consultas ilegibles).

**Elección.** `DECIMAL(20,2)`, precisión y escala configurables en
`config/pipeline.yml`. Redondeo bancario (*half-even*) para no sesgar las sumas
agregadas.

**Justificación.** 20 dígitos con 2 decimales cubren hasta 10¹⁸, muy por encima
de cualquier importe plausible. El cast desde `DOUBLE` ocurre una sola vez, en
staging; a partir de ahí todo es exacto. El `row_hash` usa
`CAST(amount AS VARCHAR)` sobre el `DECIMAL`, que produce siempre dos decimales:
la representación es estable y el hash reproducible.

**Consecuencias.** Verificado con prueba: la suma de diez importes de 0,10 da
exactamente 1,00. Los `NaN` e infinitos se detectan **antes** del cast y van a
cuarentena, porque `TRY_CAST` los convertiría silenciosamente en nulo.

**Coste.** Ninguno perceptible a este volumen. Los importes con más de 2 decimales
se redondean y se marcan con `AMOUNT_PRECISION_LOSS` (0 casos en los archivos
entregados).

---

## ADR-007 · Cuarentena con la fila original, no descarte

**Contexto.** El 3% de las filas tiene `amount` nulo. Hay que decidir qué hacer
con ellas.

**Alternativas.** Descartarlas (pierde trazabilidad e impide investigar el
origen). Imputarlas a cero (inventa datos financieros: inaceptable). Conservarlas
en el estado vigente (contamina todas las agregaciones con filas que no suman).

**Elección.** `rejected_records`, con la **fila original completa en JSON**, el
código de error, la severidad, el `run_id`, el archivo de origen, su hash, la
fecha del corte y el número de fila.

**Justificación.** El enunciado pide identificar y documentar las
inconsistencias. Una fila rechazada de la que solo queda un contador no se puede
investigar. Con el JSON original, cualquiera puede reconstruir exactamente qué
llegó.

**Consecuencias.** Se cumple siempre
`rows_read = rows_valid + rows_exact_dupes + rows_rejected`, donde `rows_valid`
cuenta filas distintas. Es uno
de los 12 controles de reconciliación. En los datos reales
`rows_exact_dupes=0`. La política es reversible:
`quality.null_amount_policy: keep_with_warning`.

---

## ADR-008 · Idempotencia por `(hash del contenido, fecha del corte)`

**Contexto.** El pipeline corre a diario. Reprocesar por reintento, por
duplicación del archivo o por error humano no puede corromper el estado.

**Alternativas.**

| Opción | Por qué se descartó |
| --- | --- |
| Por nombre de archivo | El mismo nombre puede traer contenidos distintos (reenvío corregido). |
| Solo por hash del contenido | **Descartada tras encontrar el fallo**: si un corte posterior tuviera contenido idéntico a uno anterior (el origen revierte a un estado previo), el pipeline lo ignoraría en silencio y perdería una reversión legítima. |
| Por `(hash del contenido, fecha del corte)` | Elegida. |

**Elección.** `PRIMARY KEY (source_file_hash, snapshot_date)` en `file_registry`.

**Justificación.** Reenviar el mismo archivo para el mismo corte es una
repetición: debe ser un no-op. El mismo contenido con fecha de corte posterior es
un corte legítimo que revierte el estado: debe aplicarse. Distinguir ambos casos
requiere las dos dimensiones.

**Consecuencias.** El `run_id` es determinista y legible: `<AAAAMMDD>__<hash12>`.
Un archivo con el mismo nombre y contenido distinto se procesa como corte nuevo
(política configurable). Un corte anterior al último aplicado se rechaza.
Verificado con los archivos reales: la segunda ejecución no altera ni una fila.

---

## ADR-009 · Streamlit en un perfil aparte de Docker Compose

**Contexto.** El enunciado pide insights y analítica "en el formato deseado" y
que `docker compose up --build` ejecute el pipeline.

**Alternativas.** Solo reportes CSV/Markdown (más simple, menos explorable).
HTML autocontenido con Chart.js (sin servidor, pero sin consultas ad-hoc).
Metabase o Superset (otro servicio pesado y configuración adicional: incompatible
con "sin configuración adicional").

**Elección.** Streamlit en el perfil `dashboard`, más reportes CSV y Markdown
generados siempre por el pipeline.

**Justificación.** El perfil aparte es lo que hace que `docker compose up --build`
**termine** en vez de quedarse colgado sirviendo una web. Quien quiera el tablero
ejecuta `docker compose --profile dashboard up --build`, que además espera a que
el pipeline acabe correctamente (`service_completed_successfully`).

**Consecuencias.** El tablero abre la base en **solo lectura**: no puede alterar
el resultado del pipeline. Tiene una prueba de humo (`tests/test_dashboard.py`)
que lo ejecuta entero con `AppTest` y falla si cualquier consulta se rompe.

---

## ADR-010 · Las reglas de calidad viven en un contrato declarativo

**Contexto.** El enunciado advierte contra dispersar las reglas de calidad por el
código.

**Elección.** `config/data_contract.yml` es la única fuente de verdad: tipos,
nulabilidad, dominios, normalizaciones permitidas, severidades, códigos de error
y ejemplos. El SQL de staging se **genera** a partir de él.

**Justificación.** Añadir un sinónimo de `type`, un formato de fecha o un valor
al catálogo de fondos es un cambio de configuración, no de código, y no necesita
nueva prueba unitaria de la lógica.

**Consecuencias.** `src/normalization.py` construye las expresiones SQL desde el
contrato. El contrato es también documentación legible por alguien que no lea
Python.

**Coste.** Una capa de indirección entre el YAML y el SQL final. Se mitigó
manteniéndola delgada: las expresiones se generan en cuatro funciones cortas y
el SQL resultante es inspeccionable.

---

## ADR-011 · El desbordamiento a disco de DuckDB va fuera del volumen montado

**Contexto.** DuckDB necesita un directorio temporal para desbordar a disco
cuando una operación no cabe en memoria. Es lo que permite procesar cortes
mayores que la RAM.

**Elección.** `docker-compose.yml` fija `TYBA_DATABASE__TEMP_DIRECTORY=/tmp/duckdb`,
dentro del contenedor, no bajo `./data`.

**Justificación.** Es E/S temporal de alta frecuencia. Un bind mount la penaliza
mucho, especialmente en Docker Desktop sobre macOS y Windows, y además ensucia la
carpeta del usuario con archivos que no aportan nada.

**Consecuencias.** Los archivos temporales de desbordamiento desaparecen con el
contenedor. La base y los reportes, que sí interesan, siguen en el volumen
montado.

---
