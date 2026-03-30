# TP1 — Data Engineering

---

## Aclaración previa

Hola! 

Sé que ya estamos en la entrega N°2, pero durante la primera entrega me encontraba de vacaciones y no pude realizarla en ese momento. Por eso, estoy entregando ahora lo que corresponde a la **Entrega 1**. Entiendo que las entregas son opcionales hasta la tercera, así que espero que no haya problema.

Todavía no terminé la segunda entrega, pero me sería de gran ayuda si pudieras revisar esta y decirme si estoy bien encaminada.

Muchísimas gracias!

---

## Estructura de la entrega

En lugar de entregar un `.ipynb` o un único `.py`, subí un **archivo `.zip`** con el proyecto completo.

Decidí organizarlo así porque me resultaba más claro y más representativo de cómo se trabaja en proyectos reales. Espero que esté bien haberlo hecho de esta forma.

---

## API utilizada

Usé la **API de terremotos del USGS**.

> 📎 [Documentación oficial de la API](https://earthquake.usgs.gov/fdsnws/event/1/)

### Endpoints

Trabajé con **3 endpoints**:

| Endpoint | Tipo de datos | Estrategia |
|---|---|---|
| `query` | Eventos sísmicos | Incremental |
| `catalogs` | Datos estáticos | Full |
| `contributors` | Datos estáticos | Full |

El endpoint `query` me pareció el más interesante por su naturaleza dinámica: los datos se actualizan constantemente. Los otros dos (`catalogs` y `contributors`) no tanto, pero los incluí porque estaban en la documentación y permitían cubrir el requisito de datos estáticos. 


---

## Aclaraciones Técnicas

### Formato de los endpoints

Los endpoints `catalogs` y `contributors` devuelven datos en formato **XML** (no JSON). Por eso, adapté las funciones de extracción (`extract`) y el método `_get` principal para detectar automáticamente el formato de respuesta y procesarlo en consecuencia:

- `JSON` → `.json()`
- `XML` → parseo con `ElementTree`

Esto resultó en un flujo flexible que soporta ambos formatos y los convierte a DataFrame.

> _Sé que esto se aleja un poco de lo que pide la consigna, pero la API no ofrece muchos endpoints en JSON y tuve que adaptarme. De todas formas, el endpoint principal (`query`) sí está en JSON y cumple exactamente con lo requerido_ 

---

### Transformaciones realizadas

Intenté respetar la consigna de no transformar los datos o hacerlo lo mínimo posible. Las únicas transformaciones que apliqué fueron:

#### 1. Unnest de datos anidados
El endpoint `query` devuelve información dentro de un campo `properties`. Lo *aplané*, dejando cada atributo como una columna independiente.

#### 2. Eliminación de filas completamente nulas
Había filas con todos los valores en `null`. Las eliminé porque:
- Me pareció que no aportan información útil, estando todos sus valores en nulo.
- Generaban errores al intentar subirlas (MinIO no permitía filas completamente nulas)

#### 3. Columna `time` → particiones
Transformé la columna `time` a tipo `datetime` y generé las siguientes columnas de partición:

```
partition_year
partition_month
partition_day
partition_hour
```

De esta manera pude realizar las particiones del bucket. 

#### 4. Columna de geometría
La convertí a `string` para evitar problemas al momento del almacenamiento.

Fuera de estos casos, los datos se conservan tal como los devuelve la API.

---

### Particionado

Las particiones se generan a partir de la columna `time`. Si bien la consigna menciona particionar por fecha y hora, la API devuelve *timestamps* completos, por lo que decidí partir desde el nivel año hacia abajo:

```
año → mes → día → hora
```

Esto permite mantener una estructura ordenada y facilita futuras consultas o filtrados.

---

### Estrategia de extracción incremental

Para el endpoint `query` implementé una **lógica incremental basada en metadata**:

- Se guarda el último *timestamp* procesado (`last_value`)
- En cada ejecución:
  - Si es la **primera vez** → se usa un *lookback* configurable
  - Si no → se continúa desde el último valor registrado

#### Sobre la decisión del lookback

La parte más cuestionable de esta implementación es justamente qué hacer en la **primera ejecución**, cuando todavía no existe un `last_value`.

Las alternativas que consideré fueron:

- **Traer todo el histórico disponible** → descartada porque podría saturar la API y el programa
- **Consultar el primer día disponible e iterar de a 24hs** → viable, pero introduce complejidad adicional y sus propios problemas.
- **Usar un lookback fijo configurable** → la que implementé; simple y funcional, aunque implica que la primera carga solo trae datos recientes (desde ayer, por defecto)

Elegí la tercera opción por simplicidad, siendo consciente de que sacrifica completitud histórica en esa primera carga. No sé qué te parece a vos, qué podría hacer?

---

### Estrategia de escritura

#### Incremental (eventos — `query`)

Usé una estrategia de **delete + insert por partición**:

1. Se borra la partición correspondiente
2. Se insertan los nuevos datos

La elegí porque:
- Evita duplicados
- Permite re-ejecutar sin inconsistencias
- Garantiza que los datos de cada partición estén siempre actualizados
- Es más simple que implementar *merges* complejos en esta etapa

#### Full (`catalogs` y `contributors`)

Usé **modo overwrite**. Tiene sentido porque:
- Son datasets pequeños
- Son relativamente estáticos
- No tiene valor mantener versiones históricas

En cada ejecución, el contenido se reemplaza completamente.

---

## Herramientas utilizadas

- **Python** (desarrollado en PyCharm)
- **GitHub** — repositorio con todo el código
- **Herramientas de IA:** ChatGPT y Claude

> En todos los casos traté de revisar y entender lo que generaban antes de usarlo, apoyándome también en los Google Colab que compartiste en clase.

---

## Cierre

En el trabajo tengo algo de contacto con pipelines de datos, pero nunca desde un lugar
tan técnico ni construyendo uno desde cero, así que seguro tengo mucho por mejorar.

Espero tus comentarios, muchas gracias por revisar mi trabajo y por dar el curso!
