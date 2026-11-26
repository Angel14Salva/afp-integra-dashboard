"""AFP Integra — tablero en vivo sobre la zona gold (Amazon Athena).

Consulta en vivo afp_gold.v_rendimiento y afp_gold.predicciones_modelo con un usuario IAM
de solo lectura. Variables de entorno (en Render, como secretos):
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION=us-east-2,
  ATHENA_OUTPUT=s3://afp-integra-gold-672469410615/_athena/, ATHENA_DB=afp_gold, ATHENA_WORKGROUP=primary
Las consultas se guardan en caché 1 hora para no repetir costo en Athena.
"""
import os
import time
from datetime import datetime, timezone

import altair as alt
import pandas as pd
import streamlit as st

st.set_page_config(page_title="AFP Integra · Big Data UPAO", page_icon="📊", layout="wide")

DB = os.getenv("ATHENA_DB", "afp_gold")
OUT = os.getenv("ATHENA_OUTPUT", "s3://afp-integra-gold-672469410615/_athena/")
WG = os.getenv("ATHENA_WORKGROUP", "primary")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
LOCAL = os.getenv("LOCAL_CSV_DIR")  # solo para pruebas sin AWS

ROJO, GRIS = "#d62728", "#9a9a9a"
MESES = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]
mes = lambda ts: "%s-%d" % (MESES[ts.month - 1], ts.year)


def athena(sql):
    """Ejecuta una consulta en Athena y devuelve (DataFrame, datos escaneados en bytes, id)."""
    import boto3
    cli = boto3.client("athena", region_name=REGION)
    qid = cli.start_query_execution(QueryString=sql, QueryExecutionContext={"Database": DB},
                                    ResultConfiguration={"OutputLocation": OUT}, WorkGroup=WG)["QueryExecutionId"]
    while True:
        ex = cli.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        estado = ex["Status"]["State"]
        if estado == "SUCCEEDED":
            break
        if estado in ("FAILED", "CANCELLED"):
            raise RuntimeError(ex["Status"].get("StateChangeReason", estado))
        time.sleep(0.5)
    filas, cols, token = [], None, None
    while True:
        kw = {"QueryExecutionId": qid, "MaxResults": 1000}
        if token:
            kw["NextToken"] = token
        r = cli.get_query_results(**kw)
        rows = r["ResultSet"]["Rows"]
        if cols is None:
            cols = [c.get("VarCharValue") for c in rows[0]["Data"]]
            rows = rows[1:]
        filas += [[c.get("VarCharValue") for c in row["Data"]] for row in rows]
        token = r.get("NextToken")
        if not token:
            break
    return pd.DataFrame(filas, columns=cols), ex["Statistics"].get("DataScannedInBytes", 0), qid


@st.cache_data(ttl=3600, show_spinner="Consultando Amazon Athena…")
def cargar():
    if LOCAL:
        rend = pd.read_csv(os.path.join(LOCAL, "v_rendimiento.csv"))
        pred = pd.read_csv(os.path.join(LOCAL, "predicciones_modelo.csv"))
        meta = {"bytes": 0, "ids": ["local"], "hora": datetime.now(timezone.utc)}
    else:
        rend, b1, q1 = athena("SELECT fecha_mes, anio, afp, tipo_entidad, indicador, indicador_descripcion, unidad, valor, fuente "
                              "FROM v_rendimiento")
        pred, b2, q2 = athena("SELECT fecha_mes, tramo, prob_bajo_rendimiento, prediccion, real, prob_persistencia "
                              "FROM predicciones_modelo")
        meta = {"bytes": b1 + b2, "ids": [q1, q2], "hora": datetime.now(timezone.utc)}
    rend["fecha_mes"] = pd.to_datetime(rend["fecha_mes"])
    rend["anio"] = rend["anio"].astype(int)
    rend["valor"] = pd.to_numeric(rend["valor"])
    pred["fecha_mes"] = pd.to_datetime(pred["fecha_mes"])
    for c in ("prob_bajo_rendimiento", "prediccion", "real", "prob_persistencia"):
        pred[c] = pd.to_numeric(pred[c], errors="coerce")
    return rend, pred, meta


st.title("AFP Integra · rendimiento relativo del Fondo 2")
st.caption("Proyecto de Big Data y Analítica de Datos · UPAO 2026-20 · Grupo 3. "
           "Datos de la SBS y el BCRP procesados en AWS (S3 → Glue → Athena). Esta página consulta la zona gold en vivo.")

try:
    rend, pred, meta = cargar()
except Exception as exc:  # sin credenciales o sin red: se informa, no se inventan datos
    st.error(f"No se pudo consultar Athena: {exc}")
    st.stop()

tab1, tab2, tab3 = st.tabs(["Rendimiento", "Modelo", "Cómo se obtienen los datos"])

# ------------------------------------------------------------------ Rendimiento
with tab1:
    indicadores = (rend[["indicador", "indicador_descripcion", "unidad"]].drop_duplicates()
                   .sort_values("indicador").reset_index(drop=True))
    etiquetas = dict(zip(indicadores.indicador_descripcion, indicadores.indicador))
    defecto = list(etiquetas).index(next(k for k, v in etiquetas.items() if v == "rent_real_12m"))
    elegido = st.selectbox("Indicador", list(etiquetas), index=defecto)
    cod = etiquetas[elegido]
    unidad = indicadores.set_index("indicador").loc[cod, "unidad"]
    d = rend[rend.indicador == cod]
    anios = st.slider("Años", int(d.anio.min()), int(d.anio.max()), (int(d.anio.min()), int(d.anio.max())))
    d = d[d.anio.between(*anios)]

    es_rent = unidad == "%"   # la brecha solo tiene sentido para rentabilidades (no para niveles como el valor cuota)
    otras = d[d.afp.isin(["Habitat", "Prima", "Profuturo"])].groupby("fecha_mes").valor.mean()
    integ = d[d.afp == "Integra"].set_index("fecha_mes").valor
    brecha_m = (integ - otras).dropna() if es_rent else pd.Series(dtype=float)

    c1, c2, c3, c4 = st.columns(4)
    ult = d.fecha_mes.max()
    v_int = d[(d.afp == "Integra") & (d.fecha_mes == ult)].valor
    c1.metric(f"Integra · {mes(ult)}", f"{v_int.iloc[0]:,.2f} {unidad}" if len(v_int) else "—")
    if es_rent and len(brecha_m):
        c2.metric("Brecha del último mes (pp)", f"{brecha_m.iloc[-1]:+.2f}")
        c3.metric("Meses con Integra por debajo", f"{(brecha_m < 0).sum()} de {len(brecha_m)}",
                  f"{100 * (brecha_m < 0).mean():.1f} %", delta_color="off")
    else:
        c2.metric("Brecha", "no aplica", "indicador de nivel, no de rentabilidad", delta_color="off")
    c4.metric("Filas consultadas", f"{len(rend):,}")

    color = alt.condition(alt.datum.afp == "Integra", alt.value(ROJO), alt.value(GRIS))
    linea = (alt.Chart(d).mark_line().encode(
        x=alt.X("fecha_mes:T", title="Mes"),
        y=alt.Y("valor:Q", title=f"{elegido} ({unidad})"),
        detail="afp:N", color=alt.Color("afp:N", scale=alt.Scale(
            domain=["Integra", "Habitat", "Prima", "Profuturo", "Sistema"],
            range=[ROJO, "#4c78a8", "#72b7b2", "#b279a2", "#555555"]), title="Entidad"),
        strokeWidth=alt.condition(alt.datum.afp == "Integra", alt.value(3), alt.value(1.3)),
        tooltip=["afp", alt.Tooltip("fecha_mes:T", format="%b-%Y"), alt.Tooltip("valor:Q", format=",.2f")])
        .properties(height=360, title=f"{elegido} por entidad"))
    st.altair_chart(linea, use_container_width=True)

    if len(brecha_m):
        anual = brecha_m.groupby(brecha_m.index.year).mean().reset_index()
        anual.columns = ["anio", "brecha"]
        barras = (alt.Chart(anual).mark_bar().encode(
            x=alt.X("anio:O", title="Año"), y=alt.Y("brecha:Q", title="Brecha promedio"),
            color=alt.condition(alt.datum.brecha < 0, alt.value(ROJO), alt.value(GRIS)),
            tooltip=["anio", alt.Tooltip("brecha:Q", format="+.3f")])
            .properties(height=280, title="Brecha anual: Integra − promedio de Habitat, Prima y Profuturo"))
        st.altair_chart(barras, use_container_width=True)

# ------------------------------------------------------------------ Modelo
with tab2:
    prueba = pred[pred.tramo == "prueba"].sort_values("fecha_mes")
    acierto = (prueba.prediccion == prueba.real).mean()
    sig = pred[pred.tramo == "sin_etiqueta"]
    m1, m2, m3 = st.columns(3)
    m1.metric("Exactitud en prueba", f"{acierto:.3f}", f"{len(prueba)} meses (ene-2023 a jul-2026)", delta_color="off")
    m2.metric("AUC en prueba (modelo base)", "0.487", "azar = 0.50 · persistencia = 0.463", delta_color="off")
    if len(sig):
        m3.metric(f"Probabilidad para el mes siguiente a {mes(sig.fecha_mes.iloc[0])}",
                  f"{sig.prob_bajo_rendimiento.iloc[0]:.2f}", "solo ilustrativa", delta_color="off")
    st.warning("El modelo **no supera al azar** con el objetivo mensual (AUC 0.487; modelo definitivo 0.468). "
               "La probabilidad se muestra como ilustración y **no debe usarse como alerta** (informe, 4.3 y 6.3).")
    largo = prueba.melt("fecha_mes", ["prob_bajo_rendimiento", "real"], var_name="serie", value_name="v")
    largo["serie"] = largo.serie.map({"prob_bajo_rendimiento": "Probabilidad estimada", "real": "Bajo rendimiento real (1 = sí)"})
    graf = (alt.Chart(largo).mark_line(interpolate="step-after").encode(
        x=alt.X("fecha_mes:T", title="Mes en que se predice (t)"), y=alt.Y("v:Q", title="", scale=alt.Scale(domain=[0, 1])),
        color=alt.Color("serie:N", title=""), tooltip=["serie", alt.Tooltip("fecha_mes:T", format="%b-%Y"), alt.Tooltip("v:Q", format=".2f")])
        .properties(height=340, title="Probabilidad estimada frente al resultado real (t+1)"))
    st.altair_chart(graf, use_container_width=True)
    auc = pd.DataFrame({"Modelo": ["Clase mayoritaria", "Persistencia (signo actual)", "Solo inercia", "Solo contexto",
                                   "Modelo base", "Modelo definitivo (logística, contexto + texto)"],
                        "AUC prueba": [0.500, 0.463, 0.314, 0.517, 0.487, 0.468]})
    st.dataframe(auc, hide_index=True, use_container_width=True)
    st.caption("AUC de las corridas en AWS Glue (resultados/cloud/cifras_modelo_mensual.txt y cifras_modelo_definitivo.txt).")

# ------------------------------------------------------------------ Datos
with tab3:
    st.markdown(
        "- **Bronze (S3):** 65 archivos originales de la SBS, el BCRP y las memorias de las AFP, con manifiesto SHA-256.\n"
        "- **Silver (S3 + Glue):** preparación en Glue Python Shell y ETL en Glue Spark → 8 tablas Parquet, 110,407 filas.\n"
        "- **Gold (S3 + Athena):** modelo estrella (hecho_rendimiento_mensual + dim_tiempo, dim_afp, dim_indicador) y predicciones.\n"
        "- **Esta app:** consulta `afp_gold.v_rendimiento` y `afp_gold.predicciones_modelo` en Athena con un usuario IAM de solo lectura.")
    st.write(f"Última consulta a Athena: **{meta['hora']:%Y-%m-%d %H:%M} UTC** · datos escaneados: "
             f"**{meta['bytes'] / 1e6:.2f} MB** · caché de 1 hora.")
    st.caption("Ids de consulta: " + ", ".join(meta["ids"]))
    if st.button("Volver a consultar Athena ahora"):
        cargar.clear()
        st.rerun()
    st.markdown("Repositorio: [github.com/JuniorInf01/BigData-Proyecto](https://github.com/JuniorInf01/BigData-Proyecto)")
