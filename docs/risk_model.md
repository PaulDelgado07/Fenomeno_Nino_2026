# Modelo simplificado de riesgo de inundación

El proyecto calcula un índice de 0 a 100 por zona de Guayaquil. Es un modelo
demostrativo, no un modelo hidráulico profesional.

| Componente | Máximo | Regla |
| --- | ---: | --- |
| Precipitación | 35 | 45 mm/h o más es lluvia intensa; 25 mm/h o más, moderada-alta. |
| Marea | 25 | 3.2 m o más representa pleamar alta; 2.5 m o más, marea relevante. |
| Embalse | 15 | 90% o más incrementa la presión hidrológica; 80% o más, nivel elevado. |
| Vulnerabilidad local | 20 | Valor de referencia por sector, asociado a baja cota e historial de anegamiento. |
| Efecto combinado | 10 | Se agrega cuando coinciden lluvia de 25 mm/h o más y marea de 2.5 m o más. |

Clasificación: **Bajo** (<30), **Medio** (30–49), **Alto** (50–69) y
**Crítico** (70 o más).

Las fuentes `NASA GPM`, `INOCAR` y `CELEC Daule-Peripa` se simulan en la fase
de demo. El nombre y formato de cada evento permiten sustituir el simulador por
un consumidor oficial/API sin cambiar el job Spark.
