# Automatización de Validación de Antigüedad de Crédito (Genética)

Este script de Python automatiza la auditoría y validación de clientes para el sistema "Genética", integrando datos de SAP, CRM y registros históricos de límites de crédito.

##  Problema vs Solución

* **Problema:** La auditoría se realizaba de forma semi-manual con macros de Excel y fórmulas complejas. Esto generaba errores humanos, consumía varias horas de trabajo y dificultaba la trazabilidad de las fechas de origen de los créditos.
* **Solución:** Un script robusto que centraliza la lógica de negocio, normaliza datos de distintas fuentes y calcula la elegibilidad de los clientes en segundos, garantizando datos 100% precisos para la toma de decisiones.

---

##  Tecnologías Utilizadas
* **Python 3.x**
* **Pandas:** Procesamiento, limpieza y unión de dataframes.
* **NumPy:** Operaciones lógicas y manejo de valores nulos.
* **Pathlib:** Gestión profesional de rutas de archivos.

---

##  Explicación Paso a Paso del Código

El script procesa la información a través de un pipeline de datos estructurado en 5 etapas:

### 1. Normalización de Identificadores
Debido a que los sistemas (SAP/CRM) exportan datos con formatos inconsistentes, la función `to_numeric_strip_leading_zeros` limpia los "Códigos SAP". Elimina espacios, ceros a la izquierda y caracteres no numéricos para asegurar que la unión (merge) de las tablas sea perfecta.

### 2. Ingesta Inteligente
El script lee un archivo maestro de Excel y procesa tres pestañas críticas:
* **Matriz:** Estado actual de la cartera.
* **Alta:** Registro inicial de los clientes.
* **Limite:** Historial de cambios en las líneas de crédito.

### 3. Preparación y Limpieza (Data Wrangling)
En lugar de depender de nombres de columnas fijos, el código utiliza **detección por palabras clave** (ej. busca cualquier columna que contenga "límite" o "fecha"). Esto hace que el script sea resiliente a cambios en los reportes de origen. Además, aplica técnicas de `ffill` (forward fill) para completar historiales de crédito por cliente.

### 4. Lógica de Negocio: Priorización de Fechas
El corazón del proceso es la función `obtener_fecha_referencia`. El script no solo toma una fecha, sino que aplica una jerarquía lógica para determinar la **antigüedad real**:
1.  Prioriza la fecha de alta si el límite inicial fue válido (> 5).
2.  Si no existe, busca en el historial de límites autorizados.
3.  Usa como respaldo la fecha de verificación en SAP o la fecha de la matriz.

### 5. Clasificación Automática de Estatus
Finalmente, el script calcula los meses transcurridos a la fecha actual y valida tres reglas de negocio simultáneas:
* ✅ **Antigüedad:** Mínimo 3 meses de crédito.
* ✅ **Límite:** Línea de crédito actual mayor a 5.
* ✅ **Frecuencia:** Frecuencia de pago superior a 2.

**Resultado:** Genera un archivo Excel final con la columna `cumple_final`, permitiendo filtrar instantáneamente a los clientes aptos.

---

##  Ejecución

1.  Asegurarse de tener instaladas las librerías:
    ```bash
    pip install pandas numpy openpyxl
    ```
2.  Colocar el archivo `Cambio_ADN.XLSX` en la ruta especificada.
3.  Ejecutar el script:
    ```bash
    python cambio_genetica.py
    ```

---
**Desarrollado por:** Roman López | Data Analyst
