# Insights y analitica

_Generado el 2026-08-07 14:22:42 UTC a partir de `movements.duckdb`._

Todas las cifras salen de consultas sobre las tablas persistidas y son reproducibles: cada bloque indica el metodo de calculo. Los CSV asociados estan en esta misma carpeta.

> **Limitacion transversal.** Los archivos entregados no traen identificador de transaccion (la columna es `id_cliente`, con 3.000 valores distintos sobre 50.000 filas). La identidad del movimiento es una clave de negocio derivada (`id_cliente` + fecha + producto + tipo + fondo + ordinal de ocurrencia). Toda clasificacion NEW/UPDATED/DELETED depende de ese supuesto, documentado en `docs/analysis_and_assumptions.md`.

## 1. Estado vigente

| Metrica | Valor |
| --- | ---: |
| Movimientos vigentes | 47,560 |
| Clientes distintos | 3,000 |
| Rango de fechas de movimiento | 2024-09-15 - 2024-10-16 |
| Monto total vigente | 1,113,305,841,982.36 |
| Monto de entradas (IN) | 595,058,249,885.30 |
| Monto de salidas (OUT) | 518,247,592,097.06 |
| Balance neto (IN − OUT) | 76,810,657,788.24 |

**Metodo:** agregacion sobre `movements_current` filtrando `is_active = true`. **Periodo:** todos los cortes procesados. **Limitacion:** el balance neto asume que `type` distingue entrada de salida y que `amount` es siempre positivo en su sentido. Esa segunda parte NO se cumple: ver el apartado 6.

## 2. Evolucion entre cortes

| Corte | Archivo | Leidas | Validas | Rechazadas | NEW | UPDATED | DELETED | UNCHANGED | Vigentes despues |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2024-10-15 | movimientos_dia_T.parquet | 50,000 | 48,457 | 1,543 | 48,457 | 0 | 0 | 0 | 48,457 |
| 2024-10-16 | movimientos_dia_T1.parquet | 49,000 | 47,560 | 1,440 | 9,761 | 3,724 | 10,658 | 34,075 | 47,560 |

Impacto monetario acumulado por tipo de cambio:

| Tipo de cambio | Movimientos | Impacto monetario |
| --- | ---: | ---: |
| NEW | 58,218 | 1,360,264,357,155.51 |
| DELETED | 10,658 | -249,648,138,587.86 |
| UPDATED | 3,724 | 2,689,623,414.71 |

**Metodo:** `pipeline_runs` (solo ejecuciones con estado SUCCESS) y `movement_changes.amount_delta`. **Fuente:** `change_summary.csv`, `snapshot_summary.csv`.

## 3. Analisis financiero por dimension

### Por fondo

| Fondo | Movimientos | Monto total | Entradas | Salidas | Flujo neto |
| --- | ---: | ---: | ---: | ---: | ---: |
| Renta Variable | 7,035 | 164,246,030,528.08 | 87,884,714,508.82 | 76,361,316,019.26 | 11,523,398,489.56 |
| Renta Fija | 7,040 | 163,992,818,792.96 | 87,355,986,425.06 | 76,636,832,367.90 | 10,719,154,057.16 |
| Internacional | 6,762 | 161,430,762,830.17 | 85,619,588,790.16 | 75,811,174,040.01 | 9,808,414,750.15 |
| Conservador | 6,711 | 157,985,704,157.93 | 84,513,966,016.80 | 73,471,738,141.13 | 11,042,227,875.67 |
| Crecimiento | 6,714 | 155,894,821,025.99 | 82,949,291,340.44 | 72,945,529,685.55 | 10,003,761,654.89 |
| Mercado Monetario | 6,660 | 155,342,486,046.17 | 83,848,723,741.45 | 71,493,762,304.72 | 12,354,961,436.73 |
| Balanceado | 6,638 | 154,413,218,601.06 | 82,885,979,062.57 | 71,527,239,538.49 | 11,358,739,524.08 |

### Por producto

| Producto | Movimientos | Monto total | Flujo neto |
| --- | ---: | ---: | ---: |
| Fondo de Inversión | 5,920 | 141,685,434,101.06 | 6,827,505,890.92 |
| Cuenta de Ahorro | 6,017 | 141,214,932,909.77 | 6,621,297,048.61 |
| Bonos | 5,999 | 140,775,974,218.50 | 10,997,873,415.78 |
| CDT | 5,990 | 140,119,242,548.79 | 12,719,332,737.47 |
| Divisas | 5,947 | 138,059,576,960.31 | 12,217,258,106.93 |
| Acciones | 5,901 | 137,459,376,507.17 | 8,031,582,166.73 |
| ETF | 5,873 | 137,089,423,017.31 | 10,722,950,148.83 |
| Fondo de Pensión | 5,913 | 136,901,881,719.45 | 8,672,858,272.97 |

- **Producto con mas transacciones:** Cuenta de Ahorro (6,017 movimientos).
- **Producto con mayor monto:** Fondo de Inversión (141,685,434,101.06).

### Ranking de nombres comerciales

| Nombre comercial | Movimientos | Monto total |
| --- | ---: | ---: |
| (sin nombre comercial) | 7,923 | 185,436,347,391.77 |
| Scotiabank | 4,084 | 97,418,893,294.13 |
| BBVA | 3,959 | 94,231,011,155.68 |
| Itaú | 3,998 | 93,693,597,215.11 |
| Corficolombiana | 3,934 | 93,189,510,527.90 |
| Skandia | 3,949 | 92,759,935,050.07 |
| Valores Bancolombia | 4,036 | 92,617,767,371.31 |
| Davivienda | 4,017 | 92,233,107,051.84 |
| Fiduciaria Bogotá | 3,865 | 91,174,512,687.43 |
| Bancolombia | 3,947 | 90,846,259,753.23 |
| BTG Pactual | 3,848 | 89,704,900,483.89 |

**Concentracion:** el primero concentra el 16.7% del monto vigente. Con 11 valores distintos y una distribucion practicamente plana, no hay evidencia de concentracion relevante en esta dimension.

**Metodo:** `movements_current` activo agrupado por cada dimension. **Filtros:** ninguno adicional. **Limitacion:** los movimientos con `amount` nulo estan en cuarentena y por tanto no suman en ninguna de estas cifras.

## 4. Que se corrige y cuanto pesa

| Columna | Correcciones | Movimientos afectados |
| --- | ---: | ---: |
| amount | 2,290 | 2,290 |
| description | 1,478 | 1,478 |
| commercial_name | 44 | 44 |

Correcciones y bajas de mayor impacto monetario:

| Corte | Tipo | Fondo | Producto | Antes | Despues | Impacto |
| --- | --- | --- | --- | ---: | ---: | ---: |
| 2024-10-16 | UPDATED | Renta Fija | Cuenta de Ahorro | -46,115,246.52 | 47,275,701.31 | 93,390,947.83 |
| 2024-10-16 | UPDATED | Balanceado | Acciones | -48,269,149.55 | 44,913,489.51 | 93,182,639.06 |
| 2024-10-16 | UPDATED | Mercado Monetario | Bonos | -29,956,033.82 | 47,981,567.98 | 77,937,601.80 |
| 2024-10-16 | UPDATED | Renta Fija | Fondo de Inversión | -28,694,603.70 | 47,437,523.69 | 76,132,127.39 |
| 2024-10-16 | UPDATED | Conservador | Divisas | -38,877,014.76 | 35,819,436.70 | 74,696,451.46 |
| 2024-10-16 | UPDATED | Renta Variable | Bonos | -30,204,909.27 | 44,169,689.32 | 74,374,598.59 |
| 2024-10-16 | UPDATED | Internacional | Fondo de Pensión | 28,906,534.51 | -45,249,780.57 | -74,156,315.08 |
| 2024-10-16 | UPDATED | Mercado Monetario | Fondo de Pensión | 46,111,899.71 | -24,751,012.84 | -70,862,912.55 |
| 2024-10-16 | UPDATED | Balanceado | ETF | -34,141,642.83 | 35,407,249.78 | 69,548,892.61 |
| 2024-10-16 | UPDATED | Renta Variable | Bonos | -42,425,044.36 | 26,888,024.81 | 69,313,069.17 |

**Metodo:** `movement_change_fields` (detalle por columna) y `movement_changes.amount_delta`. **Fuente:** `field_change_frequency.csv`, `top_changes_by_impact.csv`.

## 5. Calidad de los datos

- Filas leidas en total: **99,000**
- Filas enviadas a cuarentena: **2,983** (3.01%)

| Codigo de rechazo | Severidad | Filas |
| --- | --- | ---: |
| NULL_AMOUNT | RECORD_ERROR | 2,983 |

Avisos no bloqueantes (el registro se conserva):

| Flag | Severidad | Ocurrencias |
| --- | --- | ---: |
| NULL_COMMERCIAL_NAME | INFO | 15,990 |
| NULL_DESCRIPTION | INFO | 8,759 |
| NEGATIVE_AMOUNT | INFO | 2,023 |
| ZERO_AMOUNT | INFO | 1,843 |
| KEY_AMBIGUOUS | WARNING | 422 |

**Metodo:** `rejected_records` y `data_quality_flags`. Ningun registro se descarta en silencio: cada fila rechazada conserva su JSON original y su codigo de error. **Fuente:** `rejections_by_code.csv`, `quality_flags.csv`.

## 6. Eventos que merecen revision

Ninguno de estos puntos afirma que exista fraude o error. Son desviaciones respecto a un criterio explicito.

| Categoria | Codigo | Casos |
| --- | --- | ---: |
| BUSINESS_RULE | NEGATIVE_AMOUNT_CONCENTRATION | 1 |
| REVIEW | DAILY_VOLUME_OUTLIER | 1 |
| CHANGE | HIGH_DELETION_SHARE | 1 |

### Signo del monto frente al sentido del movimiento

| type | Movimientos | Con monto negativo | % | Con monto cero |
| --- | ---: | ---: | ---: | ---: |
| IN | 26,328 | 989 | 3.76% | 523 |
| OUT | 21,232 | 0 | 0.00% | 380 |

**Hallazgo:** los montos negativos aparecen concentrados en un unico valor de `type`. **Interpretacion posible:** o bien existe una convencion de signo no documentada, o bien es un defecto del origen. **Decision tomada:** no se corrige ni se rechaza, porque el enunciado no define el signo; se marca con `NEGATIVE_AMOUNT` y se reporta aqui. **Evidencia:** `anomalies.csv`, `financial_summary.csv`.

**Metodo de los outliers de monto:** regla intercuartilica de Tukey con multiplicador 3.0 aplicada por combinacion (fondo, tipo), porque las escalas difieren entre fondos. Se eligio IQR sobre z-score porque no supone normalidad y es explicable ante un auditor.

## 7. Distribucion temporal

- Dia con mas movimientos: **2024-10-03** (1,589).
- Dia con menos movimientos: **2024-10-16** (304).
- Media diaria: **1,486.2** movimientos (desviacion 225.1).

**Metodo:** `v_daily_movements`, agrupando por `movement_date` (fecha del movimiento, no fecha del corte). **Fuente:** `daily_metrics.csv`.

---

## Como reproducir cualquier cifra

```bash
docker compose run --rm shell
# o, sin Docker:
python -c "import duckdb; con=duckdb.connect('data/database/movements.duckdb');\
           print(con.execute('SELECT * FROM v_summary_by_fund').df())"
```

| Reporte | Consulta / metodo |
| --- | --- |
| `change_summary.csv` | movement_changes agrupado por snapshot_date y change_type |
| `financial_summary.csv` | sumas sobre movements_current filtrado por is_active |
| `daily_metrics.csv` | movements_current activo agrupado por movement_date |
| `data_quality_metrics.csv` | pipeline_runs de ejecuciones exitosas |
| `rejections_by_code.csv` | rejected_records agrupado por codigo de error |
| `quality_flags.csv` | data_quality_flags agrupado por codigo |
| `field_change_frequency.csv` | movement_change_fields agrupado por columna |
| `snapshot_summary.csv` | una fila por corte procesado con exito |
| `anomalies.csv` | tabla anomalies generada por src/analytics.py |
| `top_movements.csv` | movimientos vigentes ordenados por |amount| |
| `top_changes_by_impact.csv` | movement_changes ordenado por |amount_delta| |
| `reconciliation_all_runs.csv` | reconciliation_results de todas las ejecuciones |

