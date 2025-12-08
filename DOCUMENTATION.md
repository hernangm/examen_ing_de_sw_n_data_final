# Documentación del Proyecto

Este documento proporciona toda la información necesaria para configurar, ejecutar y verificar el pipeline de datos de este proyecto.

**Objetivo del Trabajo**

El objetivo de este trabajo es implementar un **pipeline de datos siguiendo el patrón Medallion (Bronze–Silver–Gold)**, orquestado con **Apache Airflow**, utilizando **pandas** para limpieza inicial, **dbt** para transformaciones y tests de calidad, y **DuckDB** como motor analítico liviano.

El pipeline procesa **transacciones diarias**, valida su calidad y genera métricas agregadas a nivel cliente.

## 1. Requerimientos

El proyecto está completamente dockerizado para simplificar la gestión de dependencias. Los únicos requisitos son:

- **Docker y Docker Compose**: Para construir y orquestar los contenedores.
- **Node.js y npm (Opcional)**: Para utilizar [los atajos definidos](#9-comandos-npm-disponibles) en `package.json`. Si no se desea instalar, se pueden ejecutar los comandos de `docker-compose` directamente.

Las principales librerías de Python utilizadas dentro del entorno Docker son:

- `apache-airflow`
- `dbt-duckdb`
- `pandas`
- `pyarrow`
- Herramientas de calidad de código: `black`, `isort`, `pylint`

## 2. Instalación y Ejecución con Docker

El entorno completo se gestiona a través de Docker Compose.

Para construir y arrancar todos los servicios (Airflow, scheduler, etc.), ejecuta el siguiente comando:

```bash
npm run docker:start
```

Este comando utiliza el archivo `docker-compose.yml` para crear un contenedor con todas las dependencias y volúmenes necesarios para ejecutar el pipeline.

## 3. Variables de Entorno

Las siguientes variables de entorno se configuran en `docker-compose.yml` y son cruciales para la ejecución del pipeline:

- `DBT_PROFILES_DIR`: Apunta al directorio de perfiles de dbt (`/opt/airflow/profiles`).
- `DUCKDB_PATH`: Define la ruta donde se almacenará la base de datos de DuckDB (`/opt/airflow/warehouse/medallion.duckdb`).
- `AIRFLOW__CORE__DAGS_FOLDER`: Indica a Airflow dónde encontrar los archivos DAG (`/opt/airflow/dags`).

## 4. Obtener la Contraseña de Airflow

El comando `airflow standalone` genera una contraseña de administrador aleatoria la primera vez que se ejecuta. Para obtenerla, revisa los logs del contenedor de Airflow.

Puedes ver los logs con el siguiente comando:

```bash
npm run docker:logs
```

Busca una línea similar a esta en el output:

```text
standalone | Login with username: admin  password: <tu_contraseña_aqui>
```

Una vez obtenida, puedes acceder a la UI de Airflow en `http://localhost:8080`.

## 5. Descripción General del DAG y del Flujo de Datos

El pipeline está orquestado mediante el DAG **`medallion_pipeline`**, cuyo objetivo es procesar archivos diarios de transacciones siguiendo el patrón Medallion.

El punto de partida del flujo es un **archivo Raw** (`transactions_YYYYMMDD.csv`), que representa la llegada de datos desde un sistema fuente externo. Este archivo puede contener distintos tipos de problemas habituales en datos reales, tales como:

* Valores nulos en campos numéricos relevantes.
* Inconsistencias de formato (mayúsculas, espacios en blanco).
* Falta de estandarización en valores categóricos.

El DAG está diseñado para **absorber estas imperfecciones sin fallar**, aplicando reglas de limpieza y validación de manera progresiva a lo largo de tres tareas principales:

1. **bronze_clean**
   Corresponde a la capa Bronze del modelo Medallion. Se encarga de limpiar y estandarizar los datos crudos, eliminando registros inválidos y dejando el dataset en condiciones mínimas de calidad.

2. **silver_dbt_run**
   Representa la capa Silver. Ejecuta los modelos de dbt que exponen los datos limpios como estructuras analíticas, aplicando casteos y validaciones estructurales.

3. **gold_dbt_tests**
   Corresponde a la capa Gold. Valida reglas de negocio y de calidad más avanzadas mediante tests de dbt y genera evidencia explícita de dichas validaciones.

La ejecución es estrictamente secuencial, asegurando que cada etapa solo se ejecute si la anterior finalizó correctamente.

---

## 6. Ejecutar el DAG

Puedes disparar el DAG `medallion_pipeline` de dos maneras:

1. **Desde la UI de Airflow**:
    - Navega a `http://localhost:8080`.
    - Busca el DAG `medallion_pipeline` y activa el interruptor para despausarlo.
    - Haz clic en el botón de "Play" para iniciar una nueva ejecución.

2. **Desde la línea de comandos**:
    Utiliza el script de npm para disparar el DAG:

    ```bash
    npm run airflow:trigger
    ```

> **Nota sobre archivos faltantes**: Si no existe un archivo `transactions_YYYYMMDD.csv` para la fecha de ejecución, el primer task (`bronze_clean`) lo detectará, registrará un `warning` y omitirá la ejecución de los tasks posteriores (`silver` y `gold`) para evitar fallos innecesarios. El DAG finalizará con estado `skipped`.

## 7. Verificación de Resultados por Capa

Para verificar que cada capa del pipeline ha funcionado correctamente, puedes ejecutar comandos dentro del contenedor.

### Capa Bronze

1. **Revisa que exista el archivo Parquet limpio**:

    ```bash
    docker-compose exec airflow-webserver ls -l /opt/airflow/data/clean/
    ```

    Deberías ver un archivo como `transactions_<fecha>_clean.parquet`.

2. **Inspecciona el contenido del archivo**:

    ```bash
    npm run duckdb:cli -- -c "SELECT * FROM read_parquet('/opt/airflow/data/clean/transactions_*.parquet') LIMIT 5;"
    ```

### Capa Silver

1. **Lista las tablas y vistas en el warehouse**:

    ```bash
    npm run duckdb:cli -- -c ".tables"
    ```

    Deberías ver `stg_transactions` y `fct_customer_transactions`.

2. **Consulta la tabla de hechos para validar los cálculos**:

    ```bash
    npm run duckdb:cli -- -c "SELECT * FROM fct_customer_transactions LIMIT 10;"
    ```

### Capa Gold

1. **Revisa que se haya generado el archivo de calidad de datos**:

    ```bash
    docker-compose exec airflow-webserver ls -l /opt/airflow/data/quality/
    ```

    Deberías ver un archivo como `dq_results_<fecha>.json`.

2. **Inspecciona el contenido del JSON**:

    ```bash
    docker-compose exec airflow-webserver cat /opt/airflow/data/quality/dq_results_*.json | jq
    ```
## 8. Pruebas de Calidad de Datos (dbt Tests)

El proyecto utiliza una combinación de pruebas de dbt incorporadas y personalizadas para asegurar la calidad y la integridad de los datos a través de las capas.

### Pruebas Incorporadas (Built-in)

Estas pruebas son estándar en dbt y se configuran directamente en los archivos `schema.yml`.

- **`not_null`**: Asegura que una columna no contenga valores nulos. Se aplica a la mayoría de las columnas clave en los modelos de staging y marts.
- **`unique`**: Garantiza que todos los valores en una columna sean únicos. Se usa en identificadores como `transaction_id` y `customer_id` en la tabla final.
- **`accepted_values`**: Verifica que los valores de una columna pertenezcan a una lista predefinida. Se utiliza para la columna `status` para asegurar que solo contenga `completed`, `pending` o `failed`.
- **`relationships`**: Comprueba la integridad referencial, asegurando que los valores de una columna existan en otra tabla. Se usa para validar que todos los `customer_id` en la tabla de hechos existan en la tabla de staging.

### Pruebas Personalizadas (Custom)

Estas pruebas genéricas se definen en la carpeta `dbt/tests/generic` para validar lógica de negocio específica.

- **`non_negative`**:
  - **Descripción**: Asegura que los valores en una columna numérica no sean negativos.
  - **Uso**: Se aplica a columnas de montos y conteos como `amount`, `transaction_count`, y `total_amount_completed`.

- **`amount_consistency`**:
  - **Descripción**: Verifica que el valor de una columna sea siempre mayor o igual que el de otra.
  - **Uso**: Se utiliza para asegurar que `total_amount_all` (suma de todos los montos) sea siempre mayor o igual que `total_amount_completed` (suma solo de los completados).

- **`is_valid_status_transition`**:
  - **Descripción**: Valida una regla de negocio: una transacción no puede tener el estado `completed` si su monto es cero o negativo.
  - **Uso**: Se aplica a la columna `status` en el modelo de staging para garantizar la consistencia entre el estado y el monto de la transacción.

## 9. Ejemplo de Funcionamiento del DAG `medallion_pipeline`

Este ejemplo muestra cómo el DAG procesa un archivo de transacciones diario a través de las tres etapas del pipeline Medallion: **Bronze, Silver y Gold**.

> **Nota sobre los comandos utilizados en esta sección**  
> En esta seccion los pasos fueron
> ejecutados directamente dentro del contenedor de Airflow mediante consola,
> sin utilizar npm.Los comandos presentados a continuación son funcionalmente equivalentes.

## Etapa 1 – Capa Bronze (bronze_clean)

El DAG parte de un archivo CSV con transacciones diarias. El contenido del archivo de entrada es el siguiente:

```csv
transaction_id,customer_id,amount,status,transaction_ts
1,1001,250.50 ,completed,2025-12-05 08:10:00
2,1002,99.99,Completed,2025-12-05 09:45:00
3,1003,,failed,2025-12-05 11:00:00
4,1002,99.99,COMPLETED,2025-12-05 09:45:00
5,1004,17.40,completed ,2025-12-05 12:30:00
6,1005,62.10,pending,2025-12-05 13:15:00

Durante la ejecución de la tarea, se aplican reglas explícitas de limpieza:

Los datos limpios se guardan en disco en formato Parquet para su posterior procesamiento.

Dentro del contenedor Docker de Airflow se ejecuta el siguiente comando:

```bash
find data/clean/ | grep transactions_
```
salida obtenida:
transactions_20251201_clean.parquet

```bash
import duckdb
con = duckdb.connect()
con.execute("""
    SELECT *
    FROM read_parquet('data/clean/transactions_20251201_clean.parquet')
    LIMIT 5
""").fetchall()
```
salida obtenida: 

[(1, 1001, 250.5, 'completed', datetime.datetime(2025, 12, 5, 8, 10), datetime.date(2025, 12, 5)),
 (2, 1002, 99.99, 'completed', datetime.datetime(2025, 12, 5, 9, 45), datetime.date(2025, 12, 5)),
 (4, 1002, 99.99, 'completed', datetime.datetime(2025, 12, 5, 9, 45), datetime.date(2025, 12, 5)),
 (5, 1004, 17.4, 'completed', datetime.datetime(2025, 12, 5, 12, 30), datetime.date(2025, 12, 5)),
 (6, 1005, 62.1, 'pending', datetime.datetime(2025, 12, 5, 13, 15), datetime.date(2025, 12, 5))]

Se observa por ejemplo, que se elimina la transaccion 3 contener de datos de amount nulos y la transaccion 4 se normaliza el valor status


## Etapa 2 – Capa Silver (silver_dbt_run)

En esta etapa, los datos son expuestos mediante el modelo de staging: stg_transactions, implementado con dbt.
No se eliminan ni modifican registros; la transformación se enfoca en:
Definir explícitamente los tipos de datos (casteos).
Estandarizar la estructura del dataset.
Establecer un contrato de datos para capas posteriores.
El resultado de la capa Silver se materializa dentro del warehouse DuckDB como un modelo accesible para análisis y agregaciones.

Dentro del contenedor Docker de Airflow se ejecuta el siguiente comando:

```bash
import duckdb
con = duckdb.connect("warehouse/medallion.duckdb")
con.execute("SHOW TABLES;").fetchall()
```
salida obtenida: 
[('stg_transactions',), ('fct_customer_transactions',)]


## Etapa 3: Capa Gold (`gold_dbt_tests`)

En esta etapa se busca validar la calidad de los datos y certificar que cumplen con los estándares del negocio.
como se menciona en el apartado Pruebas de Calidad de Datos (dbt Tests)


Donde se genera un reporte de calidad en formato JSON:

```bash
find data/quality/*.json
```

salida obtenida:
dq_results_20251205.json

```bash
cat data/quality/dq_results_20251201.json
```

```json
{
    "date": "20251205",
    "status": "passed",
    "dbt_output": "...",
    "dbt_error": "..."
}
```
El contenido del archivo indica que todos los tests fueron ejecutados exitosamente:

- status: passed
- Total de tests ejecutados: 20
- Errores: 0
- Warnings: 0

Las pruebas cubren tanto validaciones estructurales (not_null, unique,
accepted_values) como reglas de negocio personalizadas y controles de
consistencia entre métricas agregadas. Este archivo constituye evidencia
reproducible del estado de calidad del pipeline.


## 9. Limpieza y Formato de Código

Se utilizan las herramientas `black`, `isort` y `pylint` para garantizar un código limpio y consistente. Se ha añadido un script `lint.ps1` que ejecuta estas herramientas.

Para ejecutar el formateo y el análisis estático de todo el código Python, utiliza el siguiente comando de npm:

```bash
npm run lint
```

Este comando ejecutará las tres herramientas en los directorios `dags/` e `include/`, mostrando los resultados en la consola.


## 10. Comandos npm Disponibles

El archivo `package.json` incluye una serie de scripts para facilitar la interacción con el entorno Docker.

### Gestión del Entorno

- `npm run docker:start`: Inicia los contenedores de Docker en segundo plano.
- `npm run docker:stop`: Detiene los contenedores.
- `npm run docker:restart`: Reinicia los contenedores.
- `npm run docker:build`: Construye o reconstruye las imágenes de los servicios.
- `npm run docker:rebuild`: Reconstruye las imágenes sin usar la caché de Docker.
- `npm run docker:logs`: Muestra los logs de todos los servicios en tiempo real.
- `npm run docker:logs:airflow`: Muestra los logs del servicio `airflow`.
- `npm run docker:ps`: Lista los contenedores en ejecución.
- `npm run docker:clean`: Detiene y elimina los contenedores y los volúmenes asociados para una limpieza completa.
- `npm run docker:exec`: Inicia una sesión de `bash` dentro del contenedor de Airflow para ejecución de comandos manuales.

### Interacción con Airflow

- `npm run airflow:trigger`: Dispara la ejecución del DAG `medallion_pipeline`.
- `npm run airflow:list-dags`: Lista todos los DAGs disponibles en Airflow.
- `npm run airflow:ui`: Abre la interfaz de usuario de Airflow en el navegador web.

### Calidad de Código y Herramientas

- `npm run lint`: Ejecuta las herramientas de formateo y análisis estático (`black`, `isort`, `pylint`).
- `npm run duckdb:cli`: Abre una sesión de CLI con la base de datos de DuckDB.

### Atajos de Desarrollo

- `npm run dev:start`: Inicia los contenedores y muestra los logs en tiempo real.
- `npm run dev:fresh`: Realiza una limpieza completa del entorno, reconstruye las imágenes y arranca los servicios.

## 11. Posibles Mejoras

A continuación se presenta un resumen de posibles mejoras para evolucionar este pipeline hacia un entorno de producción.

### Escalabilidad

- **Warehouse Distribuido**: Reemplazar DuckDB por un data warehouse en la nube como Snowflake, BigQuery o Redshift para manejar grandes volúmenes de datos.
- **Procesamiento Distribuido**: Utilizar Apache Spark o Dask para la limpieza en la capa Bronze si los archivos de entrada son demasiado grandes para Pandas.
- **Ingesta de Datos**: Migrar de archivos CSV a sistemas de colas como Kafka o Kinesis para una ingesta en tiempo real o por micro-lotes.

### Modelado de Datos

- **Esquema de Estrella**: Desarrollar un esquema de estrella en la capa Gold con tablas de hechos (`fct_transactions`) y dimensiones (`dim_customers`, `dim_date`).
- **Modelos Incrementales**: Configurar los modelos de dbt como incrementales para que solo procesen datos nuevos en cada ejecución, mejorando el rendimiento.
