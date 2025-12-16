import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# ==============================
#     CONFIGURACIÓN
# ==============================
file_path = r"C:\Users\roman.lopez\OneDrive - Ciosa Autotodo\Documentos\Cambio Genetica\cambio_genetica_py\Cambio_ADN.XLSX"
output_path = Path(file_path).with_name("resultados_cambio_ADN.xlsx")


# ==============================
#     FUNCIONES AUXILIARES
# ==============================
def to_numeric_strip_leading_zeros(s):
    try:
        if pd.isna(s):
            return np.nan
        if isinstance(s, (int, float)):
            return int(s)
        s = str(s).strip()
        if s == "":
            return np.nan
        s = ''.join(ch for ch in s if ch.isdigit())
        if s == "":
            return np.nan
        return int(s)
    except:
        return np.nan


def read_sheets(path):
    xls = pd.ExcelFile(path)
    df_matriz = pd.read_excel(xls, sheet_name="Matriz", dtype=object)
    df_altas  = pd.read_excel(xls, sheet_name="Alta", dtype=object)
    df_limite = pd.read_excel(xls, sheet_name="Limite", dtype=object)
    return df_matriz, df_altas, df_limite


# ==============================
#     PREPARACIÓN DE TABLAS
# ==============================
def prepare_matriz(df):
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    # detectar columna código SAP (cualquiera que contenga "código")
    col_sap = next((c for c in df.columns if "código" in c.lower()), df.columns[0])
    df["codigo_num"] = df[col_sap].apply(to_numeric_strip_leading_zeros)

    # detectar columna "Última verif. ext." (o similar) y guardarla como ultima_verif
    col_ultima = next((c for c in df.columns if "última verif" in c.lower() or "última" in c.lower()), None)
    df["ultima_verif"] = pd.to_datetime(df[col_ultima], errors="coerce") if col_ultima else pd.NaT

    # Límite actual (cualquier columna que contenga "límite")
    col_lim = next((c for c in df.columns if "límite" in c.lower()), None)
    df["limite_actual"] = pd.to_numeric(df[col_lim], errors="coerce") if col_lim else pd.NA

    # Fecha alta en la matriz (columna que contenga "alta")
    col_alta = next((c for c in df.columns if "alta" in c.lower()), None)
    df["fecha_alta_matriz"] = pd.to_datetime(df[col_alta], errors="coerce") if col_alta else pd.NaT

    # PagoFrecuencia (buscar por "pago" o "frecuencia")
    col_pago = next((c for c in df.columns if "pago" in c.lower() or "frecuencia" in c.lower()), None)
    df["pago_frecuencia"] = pd.to_numeric(df[col_pago], errors="coerce") if col_pago else pd.NA

    return df


def prepare_altas(df):
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    df["codigo_num"] = df["Código SAP"].apply(to_numeric_strip_leading_zeros)

    col_fecha = next((c for c in df.columns if "registrado" in c.lower() or "alta" in c.lower()), None)
    df["fecha_alta"] = pd.to_datetime(df[col_fecha], errors="coerce") if col_fecha else pd.NaT

    col_lim = next((c for c in df.columns if "límite" in c.lower()), None)
    df["limite_alta"] = pd.to_numeric(df[col_lim], errors="coerce") if col_lim else pd.NA

    return df


def prepare_limite(df):
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    # rellenar Código SAP hacia abajo y normalizar a numeric
    df["Código SAP"] = df["Código SAP"].ffill()
    df["codigo_num"] = df["Código SAP"].apply(to_numeric_strip_leading_zeros)

    col_fecha = next((c for c in df.columns if "modified" in c.lower() or "historial" in c.lower() or "fecha" in c.lower()), None)
    df["fecha"] = pd.to_datetime(df[col_fecha], errors="coerce") if col_fecha else pd.NaT

    col_lim = next((c for c in df.columns if "límite" in c.lower()), None)
    df["limite_autorizado"] = pd.to_numeric(df[col_lim], errors="coerce") if col_lim else pd.NA

    # Rellenar hacia abajo el límite autorizado dentro de cada cliente
    df["limite_autorizado"] = df.groupby("codigo_num")["limite_autorizado"].ffill()

    return df


# ==============================
#   CONSTRUCCIÓN DE RESULTADO
# ==============================
def build_result(df_matriz_prepared, df_altas, df_limite):

    # 1) obtener la primera (más antigua) fecha en limite donde limite_autorizado > 5
    df_lim_valid = df_limite[df_limite["limite_autorizado"] > 5].copy()
    df_lim_valid = df_lim_valid.sort_values(["codigo_num", "fecha"])
    fecha_lim = df_lim_valid.groupby("codigo_num")["fecha"].first()
    lim_lim = df_lim_valid.groupby("codigo_num")["limite_autorizado"].first()

    # 2) base inicial desde la matriz preparada
    df = df_matriz_prepared[["codigo_num", "limite_actual", "pago_frecuencia", "fecha_alta_matriz"]].copy()

    # 3) merge con tabla Altas (fecha_alta, limite_alta)
    df = df.merge(
        df_altas[["codigo_num", "fecha_alta", "limite_alta"]],
        on="codigo_num",
        how="left"
    )

    # 4) merge con limite (fecha_limite y limite_limite)
    df = df.merge(fecha_lim.rename("fecha_limite"), on="codigo_num", how="left")
    df = df.merge(lim_lim.rename("limite_limite"), on="codigo_num", how="left")

    # 5) merge explícito de la columna "Última verif. ext." ya preparada (ultima_verif)
    #    usamos el df_matriz_prepared que ya contiene 'codigo_num' y 'ultima_verif'
    df = df.merge(
        df_matriz_prepared[["codigo_num", "ultima_verif"]].rename(columns={"ultima_verif": "fecha_sap"}),
        on="codigo_num",
        how="left"
    )

    return df


# ==============================
#   CÁLCULO DE MESES Y ESTATUS
# ==============================
def calculate_meses_credito(df):
    hoy = pd.Timestamp.today()

    def obtener_fecha_referencia(row):
        fecha_alta = row.get("fecha_alta")
        limite_alta = row.get("limite_alta")
        fecha_limite = row.get("fecha_limite")
        fecha_sap = row.get("fecha_sap")
        fecha_matriz = row.get("fecha_alta_matriz")

        # Si fecha_alta existe
        if pd.notna(fecha_alta):
            if pd.notna(limite_alta) and limite_alta > 5:
                return fecha_alta
            if pd.notna(fecha_limite):
                return fecha_limite
            if pd.notna(fecha_sap):
                return fecha_sap
            return fecha_matriz

        # Si fecha_alta no existe
        if pd.notna(fecha_limite):
            return fecha_limite
        if pd.notna(fecha_sap):
            return fecha_sap
        return fecha_matriz

    df["fecha_base"] = df.apply(obtener_fecha_referencia, axis=1)

    def diff_months(fecha):
        if pd.isna(fecha):
            return np.nan
        months = (hoy.year - fecha.year) * 12 + (hoy.month - fecha.month)
        if hoy.day < fecha.day:
            months -= 1
        return months

    df["meses_credito"] = df["fecha_base"].apply(diff_months)

    # Validaciones de cumplimiento: meses >= 3, limite_actual > 5, pago_frecuencia > 2
    def cumple(row):
        if pd.isna(row["meses_credito"]):
            return "No cumple"
        if row["meses_credito"] < 3:
            return "No cumple"
        if pd.isna(row["limite_actual"]) or row["limite_actual"] <= 5:
            return "No cumple"
        if pd.isna(row["pago_frecuencia"]) or row["pago_frecuencia"] <= 2:
            return "No cumple"
        return "Cumple"

    df["cumple_final"] = df.apply(cumple, axis=1)

    return df


# ==============================
#              MAIN
# ==============================
def main():
    print("Leyendo archivo...")
    df_matriz_raw, df_altas_raw, df_limite_raw = read_sheets(file_path)

    print("Preparando tablas...")
    df_matriz = prepare_matriz(df_matriz_raw)
    df_altas = prepare_altas(df_altas_raw)
    df_limite = prepare_limite(df_limite_raw)

    print("Construyendo resultado...")
    df_final = build_result(df_matriz, df_altas, df_limite)

    print("Calculando meses y estatus...")
    df_final = calculate_meses_credito(df_final)

    print("Guardando archivo...")
    df_final.to_excel(output_path, index=False)
    print("Archivo generado:", output_path)


if __name__ == "__main__":
    main()
