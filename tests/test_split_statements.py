"""split_statements: separador de sentencias SQL usado por build_staging,
apply_changes y run_reconciliation para poder pasarle parametros con nombre a
cada sentencia de un script .sql con varias sentencias.

Es codigo propio (maquina de estados caracter a caracter) porque DuckDB no
expone un splitter y las alternativas evaluadas (sqlparse) agregarian una
dependencia externa solo para esto. Sin tests directos, un cambio futuro en
cualquiera de los .sql podria introducir una regresion silenciosa aqui: estos
casos blindan cada rama de la maquina de estados.

Nota de formato: split_statements() no conserva el ';' de cierre ni hace
strip() por linea, solo strip() del bloque completo -- por eso las
aserciones comparan sobre el contenido (in / startswith), no sobre el string
exacto caracter a caracter.
"""

from __future__ import annotations

from src.normalization import split_statements


def test_separa_sentencias_simples():
    sql = "SELECT 1; SELECT 2;"
    assert split_statements(sql) == ["SELECT 1", "SELECT 2"]


def test_punto_y_coma_dentro_de_comentario_de_linea_no_separa():
    # El comentario con ';' dentro no corta la sentencia: sigue siendo una
    # sola sentencia hasta el proximo ';' real (que aqui cierra "SELECT 2").
    sql = "SELECT 1; -- nota con ; dentro\nSELECT 2;"
    statements = split_statements(sql)
    assert len(statements) == 2
    assert statements[1] == "-- nota con ; dentro\nSELECT 2"


def test_punto_y_coma_dentro_de_comentario_de_bloque_no_separa():
    sql = "SELECT 1; /* nota ; con varias ; dentro */ SELECT 2;"
    statements = split_statements(sql)
    assert len(statements) == 2
    assert statements[1] == "/* nota ; con varias ; dentro */ SELECT 2"


def test_punto_y_coma_dentro_de_literal_con_comillas_simples_no_separa():
    sql = "SELECT 'a;b' AS x; SELECT 2;"
    statements = split_statements(sql)
    assert statements == ["SELECT 'a;b' AS x", "SELECT 2"]


def test_comilla_simple_escapada_preserva_el_contenido_del_literal():
    # El manejo explicito de '' (comilla escapada) hace que el resultado sea
    # identico a simplemente alternar el estado en cada comilla: para donde
    # split_statements CORTA la sentencia da lo mismo. Lo que si importa es
    # que el contenido interno del SQL (lo que se le pasa a duckdb) se
    # preserve caracter por caracter, sin perder ninguna comilla.
    sql = "SELECT 'it''s; not a separator' AS x; SELECT 2;"
    statements = split_statements(sql)
    assert statements == ["SELECT 'it''s; not a separator' AS x", "SELECT 2"]
    assert statements[0].count("'") == 4, "las 4 comillas del literal deben preservarse"


def test_punto_y_coma_dentro_de_identificador_con_comillas_dobles_no_separa():
    sql = 'SELECT 1 AS "raro;raro"; SELECT 2;'
    statements = split_statements(sql)
    assert statements == ['SELECT 1 AS "raro;raro"', "SELECT 2"]


def test_sentencias_vacias_entre_punto_y_coma_se_descartan():
    sql = "SELECT 1;;;  SELECT 2;"
    statements = split_statements(sql)
    assert statements == ["SELECT 1", "SELECT 2"]


def test_fragmento_de_solo_comentarios_se_descarta():
    # Un fragmento que es SOLO comentario (sin codigo) no genera una
    # sentencia aparte: queda absorbido en la sentencia siguiente.
    sql = "SELECT 1; -- solo un comentario\nSELECT 2;"
    statements = split_statements(sql)
    assert len(statements) == 2
    assert "SELECT 2" in statements[1]


def test_ultima_sentencia_sin_punto_y_coma_final_se_conserva():
    sql = "SELECT 1; SELECT 2"
    statements = split_statements(sql)
    assert statements == ["SELECT 1", "SELECT 2"]


def test_comentario_de_bloque_sin_cerrar_no_produce_error():
    # Caso limite: un bloque sin cerrar consume el resto del texto sin lanzar
    # excepcion (no hay garantia de SQL valido, pero split_statements no debe
    # crashear con la entrada).
    sql = "SELECT 1; /* comentario sin cerrar SELECT 2;"
    statements = split_statements(sql)
    assert len(statements) == 2
    assert statements[1].startswith("/* comentario sin cerrar")
