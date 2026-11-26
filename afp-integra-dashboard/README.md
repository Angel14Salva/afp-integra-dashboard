# AFP Integra · tablero en vivo (Streamlit + Amazon Athena)

Aplicación web del proyecto de Big Data y Analítica de Datos (UPAO 2026-20, Grupo 3). Consulta **en vivo** la zona gold
del lago en AWS (`afp_gold.v_rendimiento` y `afp_gold.predicciones_modelo` en Amazon Athena) con un usuario IAM de solo lectura.

## Ejecutar en local
```bash
pip install -r requirements.txt
export AWS_ACCESS_KEY_ID=...  AWS_SECRET_ACCESS_KEY=...  AWS_DEFAULT_REGION=us-east-2
export ATHENA_OUTPUT=s3://afp-integra-gold-672469410615/_athena/ ATHENA_DB=afp_gold ATHENA_WORKGROUP=primary
streamlit run app.py
```

## Despliegue en Render
`render.yaml` define el servicio (plan gratuito). Las claves `AWS_ACCESS_KEY_ID` y `AWS_SECRET_ACCESS_KEY` se cargan
como variables secretas en Render; **nunca** van en el código.

## Costo y seguridad
- Las consultas se guardan en caché 1 hora; cada carga escanea pocos MB en Athena (centavos de dólar).
- El usuario IAM solo puede consultar Athena, leer el catálogo y los objetos de la zona gold, y escribir resultados en `_athena/`.
