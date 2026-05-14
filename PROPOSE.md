# Proposal for Optimizing the Censo 2024 Apache Beam Pipeline

After reviewing the `gcp.py` Apache Beam pipeline and the provided `input/` datasets, I have identified several key areas where the pipeline can be optimized to improve CPU usage, reduce memory footprint, and significantly decrease network shuffle in Google Cloud Dataflow.

## 1. Eliminate Unnecessary Shuffles (Network Shuffle & CPU)

**Problem:** 
The `personas` dataset (the largest file, over 300MB in Parquet and 2.5GB in CSV) undergoes two unnecessary `GroupByKey` operations before the final join:
```python
| "GroupPersonasByKey" >> beam.GroupByKey()
...
| "GroupCodigosToPersonaByKey" >> beam.GroupByKey()
```
Each `GroupByKey` triggers a full dataset shuffle across the network. Because `personas` is grouped, passed to a `ParDo` (where it processes multiple records at once), and then grouped *again*, Dataflow is doing an excessive amount of data serialization and network transfer.

**Proposed Fix:**
Remove `GroupPersonasByKey` and `GroupCodigosToPersonaByKey` entirely. Apply the mapping function (`MapCodigosPersona`) as a 1-to-1 transformation (`beam.Map` or `beam.ParDo` on single elements), and output the `(id_vivienda|id_hogar, persona)` tuple directly into the final `CoGroupByKey` (`JoinHogarViviendaPersona`). This will eliminate two massive shuffle stages.

## 2. Optimize Side Input Pre-processing (CPU Usage)

**Problem:**
In `MapCodigos` and `MapCodigosPersona`, the pipeline retrieves descriptions from the side inputs using the following logic inside a loop that runs for every field of every row:
```python
codigo_info = codigos_otros[campo_key]
element[campo] = codigo_info._asdict()["Descripcion"]
```
Calling `_asdict()` on a namedtuple or complex object millions of times is highly inefficient and creates a significant CPU bottleneck.

**Proposed Fix:**
Pre-process the dictionaries *before* passing them as side inputs so that the values are simple strings rather than complex objects:
```python
codigos_otros = (
    p
    | "ReadCodigosOtros" >> ReadFromCsv(...)
    | "MapCodigosOtrosToKV" >> beam.Map(lambda x: (f"{x.Campo}|{x.Codigo}", x.Descripcion)) 
)
```
Then, in the `ParDo`, the lookup becomes a fast, direct access: `element[campo] = codigos_otros[campo_key]`.

## 3. Optimize Dictionary Iteration in Transforms (CPU Usage)

**Problem:**
The `MapCodigos` transform iterates over `element.keys()` to check if a mapping exists in `codigos_otros`:
```python
for campo in element.keys():
    campo_key = f"{campo}|{element[campo]}"
    if campo_key in codigos_otros: ...
```
By performing repeated key lookups instead of utilizing `.items()`, this pattern introduces unnecessary operational overhead inside a hot loop.

**Proposed Fix:**
Optimize the loop by utilizing `.items()`. Since we flattened the `codigos_otros` dictionary in the previous step, the side-input lookups are strictly $O(1)$ flat string retrievals. By iterating cleanly via `for campo, val in element.items():`, we bypass repeated hash evaluations within the `element` dictionary, leading to tangibly faster execution per row.

## 4. Fix Potential Data Loss & Memory Overhead in Joins (Memory & Correctness)

**Problem:**
In `JoinViviendaHogar`, the pipeline does:
```python
hogar = data["hogares"][0] if data["hogares"] else {}
vivienda = data["viviendas"][0] if data["viviendas"] else {}
```
If a single dwelling (`vivienda`) has multiple households (`hogares`), `data["hogares"][0]` will drop all households except the first one. This bug is confirmed by examining the `hogares_muestra.csv` sample dataset, where, for instance, `id_vivienda` 36 and `id_vivienda` 89 contain multiple households (`id_hogar` 1 and 2). Furthermore, doing `[0]` on the iterator forces some materialization.
In `JoinHogarViviendaPersona`, the pipeline does:
```python
if data["personas"]:
    personas = data["personas"][0]
```
This requires loading the entire array of people for a household into memory.

**Proposed Fix:**
Iterate directly over the generators provided by `CoGroupByKey`. Dataflow passes an iterable, and iterating through it ensures memory isn't exhausted by large keys (Data skew):
```python
# In JoinViviendaHogar
vivienda = next(iter(data["viviendas"]), {})
for hogar in data["hogares"]:
    if "id_vivienda" in hogar:
        yield TaggedOutput("ok", (f"{hogar['id_vivienda']}|{hogar['id_hogar']}", {**hogar, "vivienda": vivienda}))
```
This fixes the bug of dropped households and avoids creating large local lists.

## 5. Early Filtering (Reduce Overall Processing)

**Problem:**
The filtering logic (`beam.Filter(lambda v: v["tipo_operativo"] == 1)`) is currently commented out, likely to accommodate testing configurations. A review of the sample datasets (`viviendas_muestra.csv`, `hogares_muestra.csv`, and `personas_muestra.csv`) confirms that the `tipo_operativo` column is present and indeed contains distinct values (like 1 and 2). When disabled in a live run, the pipeline is forced to process 100% of the dataset, only to potentially write unnecessary data.

**Proposed Fix:**
When migrating the pipeline to a production environment, it is crucial to re-enable the `beam.Filter(lambda v: v["tipo_operativo"] == 1)` transformations. While underlying parquet layers theoretically support predicate pushdowns, the native `ReadFromParquet` in standard Apache Beam versions often lacks direct support for `filters=[]` arguments. Activating standard `beam.Filter` components immediately following the read operations is the most robust and universally compatible strategy to discard irrelevant rows as early as possible.

## Summary of Impact
- **Shuffles:** Reduced from 5 to 3 overall `GroupByKey` operations.
- **CPU:** Dramatically reduced by removing `_asdict()` in hot loops and limiting key iterations.
- **Memory:** Improved by streaming through `CoGroupByKey` iterables instead of materializing lists via `[0]`.
- **Correctness:** Resolved a silent data loss issue when joining 1-to-many relationships (Vivienda -> Hogares).
