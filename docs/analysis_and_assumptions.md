# Análisis, supuestos y decisiones

Este documento se escribió **antes** de implementar. Recoge lo que dice el
enunciado, lo que dicen los datos, dónde no coinciden y qué se decidió hacer al
respecto.

---

## 1. Resumen del problema

Tyba recibe un corte diario con el estado de los movimientos financieros de sus
clientes. Los cortes no son independientes: el de mañana es una evolución del de
hoy. Hay que construir un pipeline que los ingiera y mantenga una base
consultable y consistente en el tiempo, clasificando correctamente los
movimientos nuevos, corregidos, eliminados y sin cambios, sin perder
trazabilidad ni duplicar información.

El enunciado pide además:

- identificar y tratar las inconsistencias de los datos;
- diseñar pensando en volúmenes de millones de filas;
- correr con `docker compose up --build`, sobre DuckDB o PostgreSQL, sin pasos
  manuales adicionales.

Se entregan dos archivos: `movimientos_dia_T.parquet` (50.000 filas) y
`movimientos_dia_T1.parquet` (49.000 filas).

**La pista que más condiciona el diseño:** esto va a correr todos los días, no
solo dos veces. Consecuencias directas:

- descarta cualquier solución acoplada a los nombres `_T` y `_T1`;
- obliga a que reprocesar un corte no corrompa el estado (idempotencia);
- descarta cargar el dataset completo en memoria con Python puro.

También se aclara que se evalúa el criterio y la justificación de las
decisiones, no una única respuesta correcta — de ahí que este documento
explique el *por qué* de cada decisión y no solo el *qué*.

El resto queda deliberadamente abierto: lenguaje, librerías, arquitectura,
estructura de carpetas y formato de los insights son elección del
implementador. Lo único fijo es el motor (DuckDB o PostgreSQL) y el comando de
arranque.

---

## 2. Lo que dicen los datos

Perfilado completo en `data/reports/data_profile.md`, generado por
`scripts/profile_data.py`. Resumen de lo relevante para el diseño:

### 2.1 Esquema recibido

| Columna | Tipo físico | Nulos en T | Nulos en T+1 |
| --- | --- | ---: | ---: |
| `id_cliente` | string | 0 | 0 |
| `date` | string | 0 | 0 |
| `product` | string | 0 | 0 |
| `type` | string | 0 | 0 |
| `fund` | string | 0 | 0 |
| `amount` | double | 1.543 | 1.440 |
| `description` | string | 4.563 | 4.485 |
| `commercial_name` | string | 8.307 | 8.167 |

Ambos archivos: 8 columnas, `parquet-cpp-arrow 21.0.0`, un solo row group.

### 2.2 Inconsistencias encontradas

| # | Hallazgo | T | T+1 | Decisión |
| --- | --- | ---: | ---: | --- |
| D-01 | **No existe columna `id`.** La columna es `id_cliente` y tiene 3.000 valores distintos sobre 50.000 filas (16,7 filas por valor) | 3.000 | 3.000 | Ver sección 3, A-01 |
| D-02 | `date` llega en **dos formatos** en el mismo archivo: ISO y DD/MM/YYYY | 46.518 / 3.482 | 45.567 / 3.433 | Parseo multi-formato configurable |
| D-03 | `type` llega con **10 variantes textuales**: `entrada`, `Entrada`, `ENTRADA`, `in`, `IN`, `salida`, `Salida`, `SALIDA`, `out`, `OUT` | 10 | 10 | Canonicalización a `IN`/`OUT` |
| D-04 | `fund` llega con **23 variantes** de 7 valores reales: mayúsculas, espacios externos, dobles espacios internos | 23 → 7 | 23 → 7 | Canonicalización contra catálogo |
| D-05 | `fund` con espacios externos | 1.483 | 1.478 | `trim` |
| D-06 | `fund` con dobles espacios internos (`Mercado  Monetario`) | 1.477 | 1.472 | Colapso de espacios |
| D-07 | `amount` nulo | 1.543 (3,09%) | 1.440 (2,94%) | Cuarentena (ver A-04) |
| D-08 | `amount` negativo, **exclusivamente en `type = IN`** | 1.034 | 989 | Se marca y se reporta, no se corrige (ver A-05) |
| D-09 | `amount` igual a cero | 940 | 903 | Se marca, se conserva |
| D-10 | Duplicados exactos de fila completa | 0 | 0 | Lógica implementada y probada igualmente |
| D-11 | Misma clave de negocio con datos distintos | 116 grupos / 232 filas | 105 / 210 | Ordinal determinista (ver A-02) |
| D-12 | `description` nulo | 4.563 | 4.485 | Permitido, se marca como INFO |
| D-13 | `commercial_name` nulo | 8.307 | 8.167 | Permitido, se marca como INFO |
| D-14 | `product` limpio: 8 valores, sin variantes | 8 | 8 | Solo `trim` preventivo |
| D-15 | `id_cliente` siempre cumple `^CLI[0-9]{6}$` | 100% | 100% | Se valida como aviso, no como error |
| D-16 | `amount` no tiene más de 2 decimales | 0 filas | 0 filas | Política de redondeo definida por si aparece |
| D-17 | Rango de fechas: T llega hasta 2024-10-15; T+1 hasta 2024-10-16 | 31 días | 32 días | Ancla la fecha de corte de cada archivo |
| D-18 | Ningún campo de texto contiene los caracteres de control U+001E / U+001F | 0 | 0 | Se pueden usar como separadores de hash sin ambigüedad |

### 2.3 El hallazgo que condiciona todo el diseño

El glosario del enunciado documenta:

> `id` - Identificador de la transacción

Esa columna **no existe** en los archivos. La que hay es `id_cliente`, y no es
un identificador de transacción: 3.000 valores distintos repartidos en 50.000
filas. Los 3.000 valores son además **idénticos** en ambos cortes (0 altas, 0
bajas a nivel de `id_cliente`).

Consecuencia: si se tomara `id_cliente` como el `id` del enunciado, el resultado
sería 0 NEW, 0 DELETED y 3.000 UPDATED, lo que no responde a ninguno de los
cuatro casos que pide el ejercicio. **Hay que derivar una identidad de
movimiento.**

---

## 3. Supuestos

Cada supuesto indica qué se asumió, por qué, con qué evidencia, y qué pasaría si
fuera falso.

### A-01 · La identidad del movimiento es una clave de negocio derivada

**Supuesto.** Un movimiento se identifica por la combinación normalizada de
`id_cliente`, `date`, `product`, `type` y `fund`, más un ordinal de ocurrencia
que desempata movimientos legítimamente repetidos.

**Por qué esos cinco campos y no otros.** Alineación fila a fila de los dos
archivos (`difflib.SequenceMatcher` sobre las tuplas completas):

- 35.000 filas idénticas, 15.000 desaparecidas, 14.000 aparecidas;
- cruzando esas 15.000 desaparecidas contra las 14.000 aparecidas con la clave
  de los cinco campos normalizados: **~3.900 pares** que comparten clave y
  difieren solo en `amount`, `description` y/o `commercial_name` — la
  definición exacta de "registro corregido" del enunciado.

Esos ~3.900 pares son de la exploración previa, sobre filas crudas. El
pipeline, sobre filas ya validadas y con el ordinal de ocurrencia aplicado,
clasifica **3.724 `UPDATED`** (`amount` 2.290, `description` 1.478,
`commercial_name` 44) — cifra reproducible en `insights.md` y
`pipeline_runs.rows_updated`. La diferencia son filas con `amount` nulo: la
exploración las emparejaba, el pipeline las manda a cuarentena.

La partición resultante cuadra con los totales:

```
T   = 35.000 sin cambios + ~4.000 corregidos + ~11.000 eliminados = 50.000  ✓
T+1 = 35.000 sin cambios + ~4.000 corregidos + ~10.000 nuevos     = 49.000  ✓
```

**Sin normalizar, la identidad se rompe.** De los ~3.900 pares, 1 tenía la
`date` cruda en otro formato (ISO vs DD/MM/YYYY) y 2 tenían el `fund` crudo en
otras mayúsculas. Por eso la normalización va antes del cálculo de la clave.

**Si el supuesto es falso** — el origen real trae un identificador de
transacción que no llegó en la muestra —, la clave derivada se sustituye
cambiando una lista en `config/data_contract.yml` (`identity.business_key`).
No hay que tocar SQL: el `PARTITION BY` y el hash de `movement_key` en
`sql/staging.sql` se generan desde `identity.business_key` vía
`src/normalization.py::business_key_columns` / `business_key_hash_parts_expr`.
`tests/test_business_key_config.py` lo verifica: reducir la clave a
`[id_cliente, date, product]` hace que dos filas antes distintas caigan en la
misma partición.

**Alternativas descartadas** (detalle en `decisions.md`, ADR-002): usar
`id_cliente` como id, y usar el hash de la fila completa como identidad.

### A-02 · Las claves repetidas se desempatan con un ordinal determinista

**Supuesto.** Cuando dos movimientos comparten clave de negocio dentro de un
mismo corte (116 grupos en T, 105 en T+1), se les asigna un ordinal 1, 2, … a
partir de un orden determinista por `amount`, `description` y `commercial_name`.

**Por qué.** Son movimientos reales del mismo cliente, el mismo día, el mismo
producto, fondo y sentido, pero de importe distinto. No son duplicados: son
transacciones distintas. Descartarlos perdería datos (232 + 210 filas) y
elegir una arbitrariamente falsearía las sumas.

**Coste asumido y documentado.** Si dentro de un grupo ambiguo uno de los
importes cambia entre cortes, el ordinal puede reasignarse y aparecer **dos**
correcciones en vez de una. Afecta como máximo al 0,45% de las filas. Todas esas
filas quedan marcadas con `is_key_ambiguous = true` y con el flag
`KEY_AMBIGUOUS` en `data_quality_flags`, de modo que el impacto es medible.

**Alternativa descartada.** Mandar todo el grupo a cuarentena en lugar de
desempatar con el ordinal. Es más conservador pero pierde datos reales sin
necesidad; por eso no se implementó. (Antes existía una opción
`quality.conflicting_duplicate_policy: quarantine` en `pipeline.yml` que
sugería que esto era un interruptor configurable — no lo era, ningún código la
leía, y se retiró del YAML por eso mismo.)

### A-03 · Los cortes son completos, no incrementales

**Supuesto.** Cada archivo contiene el estado **completo** de los movimientos en
esa fecha, no solo los cambios del día.

**Evidencia.** El enunciado dice "cada día recibimos un archivo con el estado de
los movimientos" y define "registro eliminado" como "un id que existía en T no
aparece en T+1". Esa definición solo tiene sentido si el corte es completo.

**Riesgo.** Si un día llegara un corte parcial o truncado, la solución
interpretaría todo lo ausente como eliminado. Por eso existen las guardas de
`config/pipeline.yml → guards`, que abortan la carga sin aplicar bajas cuando el
volumen o el porcentaje de bajas se salen de lo esperado (ver A-06).

### A-04 · Un movimiento sin importe no es reconciliable y va a cuarentena

**Supuesto.** `amount` nulo invalida el registro (3,09% en T, 2,94% en T+1).

**Por qué.** Es un dato financiero: sin importe no se puede sumar, ni conciliar,
ni calcular impacto de una corrección. Mantenerlo en el estado vigente
contaminaría todas las agregaciones con una fila que no aporta valor y que
haría que las sumas no cuadraran con el número de movimientos.

**Cómo se mitiga.** No desaparece: va a `rejected_records` con su fila original
completa en JSON, código `NULL_AMOUNT`, `run_id` y archivo de origen.
Consultable, reportado en `rejections_by_code.csv`.

**Alternativa disponible.** `config/pipeline.yml → quality.null_amount_policy: keep_with_warning`
lo conserva marcado con el flag `NULL_AMOUNT_KEPT`. No es el valor por defecto
porque prioriza el volumen sobre la fiabilidad de las cifras.

### A-05 · No se infiere ninguna regla sobre el signo de `amount`

**Supuesto.** El enunciado no define el signo, así que no se impone ninguno.

**Hallazgo.** Los importes negativos aparecen **exclusivamente** en `type = IN`:
1.034 de 1.034 en T, 989 de 989 en T+1. Ningún `OUT` es negativo. Esa
concentración es demasiado limpia para ser casual.

**Decisión.** No se corrige ni se rechaza. Se marca con `NEGATIVE_AMOUNT`
(severidad INFO) y se registra como anomalía
`NEGATIVE_AMOUNT_CONCENTRATION` en la tabla `anomalies`, con el porcentaje
observado y una nota explicando que puede ser una convención de signo no
documentada o un defecto de origen. **Es una pregunta para el negocio, no una
decisión de ingeniería.**

**Qué se evitó.** Escribir `if type = 'IN' then amount = abs(amount)` habría
alterado 1.034 importes reales basándose en una regla que nadie enunció.

### A-06 · Los umbrales de las guardas son controles operativos, no reglas de negocio

**Supuesto.** Un corte que da de baja más del 30% del estado vigente, o que trae
más de un 30% menos de filas que el anterior, o que mueve el monto total más de
un 50%, es sospechoso de estar truncado o corrupto.

**Calibración.** Contra lo observado entre T y T+1 (21,99% de bajas, 2,0% de
caída de volumen):

- umbral de **aviso**: 10% — sobre los datos reales, la alerta se dispara
  (queda en `run_alerts` y visible en el tablero);
- umbral de **fallo**: 30% — no se dispara.

El mecanismo queda demostrado funcionando sin bloquear la entrega.

**Importante.** 21,99% de bajas diarias es altísimo para un feed financiero real.
Se reporta como anomalía `HIGH_DELETION_SHARE`. En un sistema en producción
merecería investigación; aquí es una característica del dataset del ejercicio.

**Salvaguarda añadida.** Los umbrales porcentuales solo se aplican por encima de
`guards.min_rows_for_ratio_guards` (100 filas). Con 3 movimientos vigentes, una
baja es el 33% y ese porcentaje no informa de nada.

### A-07 · La fecha del corte de los archivos entregados

**Supuesto.** `movimientos_dia_T.parquet` corresponde al corte 2024-10-15 y
`movimientos_dia_T1.parquet` al 2024-10-16.

**Por qué.** Los nombres no contienen fecha. Se ancla al `max(date)` observado
en cada archivo (exactamente 2024-10-15 y 2024-10-16, que además da el orden
correcto entre ambos), declarado en `config/pipeline.yml →
ingestion.declared_sequence` — no adivinado en el código.

**Para cortes futuros**, orden de prioridad:

1. `--snapshot-date` en la CLI
2. fecha ISO en el nombre del archivo
3. secuencia declarada
4. `max(date)` del contenido (con aviso)

Un archivo llamado `movimientos_2026-08-05.parquet` funciona sin tocar nada.

### A-08 · `date` es la fecha del movimiento, no la de actualización

**Supuesto.** `date` describe cuándo ocurrió el movimiento y no cambia cuando el
movimiento se corrige.

**Evidencia.** En los pares corregidos, la fecha normalizada es idéntica en
los dos cortes. Nunca cambia.

**Consecuencia.** No existe ninguna marca temporal de actualización en los datos.
Por eso, ante varias versiones de una misma clave dentro de un mismo corte, **no
hay criterio confiable de recencia** y se aplica A-02 en vez de "quedarse con la
más nueva".

### A-09 · Un valor categórico nuevo es crecimiento del negocio, no un error

**Supuesto.** Si aparece un `fund` o un `product` que no está en el catálogo
observado, se conserva normalizado y se marca; no se rechaza.

**Por qué.** Un fondo nuevo es una situación de negocio normal. Rechazarlo
perdería datos válidos. `type`, en cambio, sí rechaza los valores desconocidos:
sin saber si un movimiento entra o sale, el registro no es utilizable.

---

## 4. Ambigüedades que el enunciado no resuelve

| # | Ambigüedad | Cómo se resolvió | Reversible por |
| --- | --- | --- | --- |
| Q-01 | ¿Qué identifica a una transacción, si no hay `id`? | Clave de negocio derivada + ordinal (A-01) | `data_contract.yml → identity.business_key` |
| Q-02 | ¿Los valores válidos de `type` son solo `IN`/`OUT`? | Se derivó el catálogo de los datos: 10 variantes → 2 canónicos | `data_contract.yml → type_synonyms` |
| Q-03 | ¿`amount` debe ser positivo? | No se asume nada; se reporta la concentración observada (A-05) | - |
| Q-04 | ¿Qué significa exactamente `date`? | Fecha del movimiento (A-08) | - |
| Q-05 | ¿Un movimiento ausente en T+1 está cancelado o es un fallo de extracción? | Baja lógica + guardas de volumen (A-03, A-06) | `pipeline.yml → guards` |
| Q-06 | ¿Un movimiento eliminado puede reaparecer? | Sí: se soporta como `REACTIVATED` | `pipeline.yml → change_detection.allow_reactivation` |
| Q-07 | ¿Qué hacer con importes de más de 2 decimales? | Redondeo bancario (half-even) y flag; no ocurre en los datos actuales | `pipeline.yml → normalization.amount` |
| Q-08 | ¿En qué orden procesar los cortes? | Por fecha de corte; se rechaza un corte anterior al último aplicado | `pipeline.yml → ingestion.allow_out_of_order` |
| Q-09 | ¿Cuántos rechazos son demasiados? | Aviso al 2%, fallo al 15% (observado: ~3%) | `pipeline.yml → quality.max_rejection_rate` |

---

## 5. Riesgos

| # | Riesgo | Mitigación implementada |
| --- | --- | --- |
| RG-01 | La clave de negocio derivada no coincide con la identidad real del origen | Clave configurable en un solo sitio; supuesto documentado y medido; el ordinal marca las filas dudosas |
| RG-02 | Un corte truncado se interpreta como 10.000 bajas | Guardas con umbrales de aviso y fallo; en caso de fallo **no se aplica nada** y la ejecución queda `FAILED_GUARD` |
| RG-03 | Reprocesar un archivo duplica el histórico | Idempotencia por `(hash del contenido, fecha del corte)`; probada con los archivos reales |
| RG-04 | Un fallo a mitad de escritura deja la base inconsistente | Toda la escritura de GOLD en una transacción; pruebas de rollback y de fallo poscommit |
| RG-05 | Errores de coma flotante en los importes | `DECIMAL(20,2)` en toda la persistencia; probado que 10 × 0,10 = 1,00 exacto |
| RG-06 | El ordinal de desempate se reasigna y genera correcciones falsas | Filas marcadas con `is_key_ambiguous`; alternativa de cuarentena disponible |
| RG-07 | El origen cambia el esquema sin avisar | Validación de esquema contra el contrato; columna obligatoria ausente = error crítico que detiene el pipeline |
| RG-08 | Crecimiento a millones de filas | Todo el trabajo en SQL sobre DuckDB; medido hasta 10M filas por corte (ver README sección 13) |
| RG-09 | Las conclusiones analíticas se toman como verdades de negocio | Cada insight indica método, periodo, filtros y limitación; las anomalías se declaran explícitamente como "no es una afirmación de fraude ni de error" |

---

## 6. Decisiones propuestas (resumen)

Detalle completo en [`decisions.md`](decisions.md).

| ADR | Decisión |
| --- | --- |
| ADR-001 | DuckDB como motor de persistencia y de cómputo |
| ADR-002 | Clave de negocio derivada + ordinal de ocurrencia como identidad |
| ADR-003 | Arquitectura de tres capas ligera (Bronze / Silver / Gold) dentro de una sola base |
| ADR-004 | SCD tipo 2 con baja lógica para el histórico |
| ADR-005 | Guardas configurables de dos niveles (aviso / fallo) |
| ADR-006 | `DECIMAL(20,2)` en toda la persistencia monetaria |
| ADR-007 | Cuarentena con la fila original en lugar de descarte |
| ADR-008 | Idempotencia por `(hash del contenido, fecha del corte)` |
| ADR-009 | Streamlit para el tablero, en un perfil aparte de Docker Compose |
| ADR-010 | Reglas de calidad en un contrato declarativo, fuera del código |
| ADR-011 | El desbordamiento a disco de DuckDB va fuera del volumen montado |

---

## 7. Criterios de aceptación

La solución se considera terminada cuando:

| # | Criterio | Cómo se verifica |
| --- | --- | --- |
| C-01 | Los cuatro casos del enunciado se detectan | `tests/test_change_detection.py::test_los_cuatro_casos_del_enunciado` |
| C-02 | Los archivos originales no se modifican | El pipeline solo abre `data/raw` en lectura; el hash se registra en `file_registry` |
| C-03 | Existe como máximo una fila vigente por movimiento | PRIMARY KEY + control de reconciliación + invariante |
| C-04 | El histórico conserva todas las versiones | `tests/test_persistence.py` |
| C-05 | Ningún registro inválido desaparece en silencio | `rows_read = rows_valid + rows_exact_dupes + rows_rejected` verificado en cada ejecución |
| C-06 | Las operaciones críticas son transaccionales | `tests/test_transactions.py`, 7 pruebas |
| C-07 | El pipeline es idempotente | `tests/test_idempotency.py` + prueba con los archivos reales |
| C-08 | Todas las reconciliaciones cuadran | 12 controles por ejecución, persistidos en `reconciliation_results` |
| C-09 | Los importes usan tipos decimales exactos | `tests/test_reconciliation.py::test_la_suma_monetaria_se_conserva_exactamente` |
| C-10 | Un error crítico detiene la ejecución con código distinto de 0 | `tests/test_pipeline.py` |
| C-11 | Se admiten T+2, T+3 … sin tocar el código | `tests/test_change_detection.py::test_tres_cortes_consecutivos` |
| C-12 | `docker compose up --build` funciona sin configuración adicional | Ver `README.md` sección 4 |
| C-13 | Los insights salen de consultas reproducibles | Cada métrica de `insights.md` indica su método |
| C-14 | La documentación coincide con la implementación | Matriz de trazabilidad en `estudio/requirements_traceability.md` |

---

## 8. Preguntas que este documento no puede resolver

Requieren respuesta del negocio:

1. **¿Cuál es el identificador real de una transacción?** Es la pregunta más
   importante. La clave derivada funciona sobre estos datos, pero un identificador
   del origen sería más robusto.
2. **¿Qué significa un importe negativo en un movimiento de entrada?** El 100% de
   los negativos está en `type = IN`. ¿Convención de signo o defecto de origen?
3. **¿Es normal que desaparezca el 22% de los movimientos de un día para otro?**
   Para un feed financiero real sería alarmante.
4. **¿Qué se espera de un movimiento sin importe?** Hoy va a cuarentena; si el
   negocio los considera válidos, es un cambio de una línea de configuración.
5. **¿Los cortes son siempre completos?** Todo el tratamiento de bajas depende de
   ello.
6. **¿Un movimiento eliminado puede reaparecer legítimamente?** Está soportado,
   pero no ocurre en los datos entregados.
