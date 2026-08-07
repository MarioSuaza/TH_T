# Reconciliacion - ejecucion `20241016__ee067cc5bee0`

- **Corte:** 2024-10-16
- **Resultado global:** TODOS LOS CONTROLES CUADRAN

## Volumen

| Metrica | Valor |
| --- | ---: |
| Filas leidas del archivo | 49,000 |
| Filas validas (distintas) | 47,560 |
| Duplicados exactos colapsados | 0 |
| Filas rechazadas (cuarentena) | 1,440 |
| Claves de negocio ambiguas | 202 |

## Clasificacion de cambios

| Tipo | Movimientos |
| --- | ---: |
| NEW | 9,761 |
| UPDATED | 3,724 |
| DELETED | 10,658 |
| UNCHANGED | 34,075 |
| REACTIVATED | 0 |
| Ya dados de baja (sin cambio) | 0 |

## Estado vigente

| Metrica | Antes | Despues |
| --- | ---: | ---: |
| Movimientos activos | 48,457 | 47,560 |
| Monto vigente | 1,133,406,868,796.56 | 1,113,305,841,982.36 |
| Monto IN | 604,445,045,436.15 | 595,058,249,885.30 |
| Monto OUT | 528,961,823,360.41 | 518,247,592,097.06 |

## Controles

| Grupo | Control | Izquierda | Derecha | Diferencia | Resultado |
| --- | --- | ---: | ---: | ---: | :---: |
| BITACORA | eventos registrados = cambios reales detectados | movement_changes=24,143.00 | cambios_reales=24,143.00 | 0.00 | OK |
| BITACORA | toda correccion tiene al menos una columna modificada registrada | correcciones_sin_detalle=0.00 | cero=0.00 | 0.00 | OK |
| CAMBIOS | new+updated+deleted+unchanged+reactivated+ya_dados_de_baja = claves del FULL OUTER JOIN | suma_por_tipo=58,218.00 | claves_join=58,218.00 | 0.00 | OK |
| CAMBIOS | new+updated+unchanged+reactivated = claves distintas del corte | clasificadas_del_corte=47,560.00 | staging_distintas=47,560.00 | 0.00 | OK |
| ESTADO | como maximo una fila vigente por movement_key | claves_duplicadas=0.00 | cero=0.00 | 0.00 | OK |
| ESTADO | todas las claves del corte estan en el estado vigente y activas | sin_reflejo=0.00 | cero=0.00 | 0.00 | OK |
| ESTADO | vigentes_despues = vigentes_antes + nuevos + reactivados - eliminados | esperado=47,560.00 | observado=47,560.00 | 0.00 | OK |
| HISTORICO | como maximo una version marcada como vigente por movement_key | versiones_vigentes_duplicadas=0.00 | cero=0.00 | 0.00 | OK |
| HISTORICO | versiones consecutivas sin huecos | claves_con_hueco=0.00 | cero=0.00 | 0.00 | OK |
| INGESTA | filas_leidas = validas + rechazadas | rows_read=49,000.00 | valid+rejected=49,000.00 | 0.00 | OK |
| MONETARIA | monto_vigente_despues = monto_vigente_antes + suma(impacto de los cambios) | esperado=1,113,305,841,982.36 | observado=1,113,305,841,982.36 | 0.00 | OK |
| MONETARIA | suma de montos del corte = suma de montos de sus claves en el estado vigente | staging=1,113,305,841,982.36 | vigente=1,113,305,841,982.36 | 0.00 | OK |

## Registros en cuarentena

| Codigo | Severidad | Filas |
| --- | --- | ---: |
| NULL_AMOUNT | RECORD_ERROR | 1,440 |

## Sumas por dimension (estado vigente)

| Dimension | Valor | Movimientos | Monto total |
| --- | --- | ---: | ---: |
| type | IN | 26,328 | 595,058,249,885.30 |
| type | OUT | 21,232 | 518,247,592,097.06 |
| fund | Renta Variable | 7,035 | 164,246,030,528.08 |
| fund | Renta Fija | 7,040 | 163,992,818,792.96 |
| fund | Internacional | 6,762 | 161,430,762,830.17 |
| fund | Conservador | 6,711 | 157,985,704,157.93 |
| fund | Crecimiento | 6,714 | 155,894,821,025.99 |
| fund | Mercado Monetario | 6,660 | 155,342,486,046.17 |
| fund | Balanceado | 6,638 | 154,413,218,601.06 |
| product | Fondo de Inversión | 5,920 | 141,685,434,101.06 |
| product | Cuenta de Ahorro | 6,017 | 141,214,932,909.77 |
| product | Bonos | 5,999 | 140,775,974,218.50 |
| product | CDT | 5,990 | 140,119,242,548.79 |
| product | Divisas | 5,947 | 138,059,576,960.31 |
| product | Acciones | 5,901 | 137,459,376,507.17 |
| product | ETF | 5,873 | 137,089,423,017.31 |
| product | Fondo de Pensión | 5,913 | 136,901,881,719.45 |
| commercial_name | (sin nombre) | 7,923 | 185,436,347,391.77 |
| commercial_name | Scotiabank | 4,084 | 97,418,893,294.13 |
| commercial_name | BBVA | 3,959 | 94,231,011,155.68 |
| commercial_name | Itaú | 3,998 | 93,693,597,215.11 |
| commercial_name | Corficolombiana | 3,934 | 93,189,510,527.90 |
| commercial_name | Skandia | 3,949 | 92,759,935,050.07 |
| commercial_name | Valores Bancolombia | 4,036 | 92,617,767,371.31 |
| commercial_name | Davivienda | 4,017 | 92,233,107,051.84 |
| commercial_name | Fiduciaria Bogotá | 3,865 | 91,174,512,687.43 |
| commercial_name | Bancolombia | 3,947 | 90,846,259,753.23 |
| commercial_name | BTG Pactual | 3,848 | 89,704,900,483.89 |
