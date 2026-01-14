import apache_beam as beam
import logging
import random
from apache_beam.options.pipeline_options import PipelineOptions

logging.basicConfig(level=logging.INFO)


class JoinPersonaVivienda(beam.DoFn):
    def __init__(self):
        self.element_counter = beam.metrics.Metrics.counter(
            'join_persona_vivienda', 'processed_elements')

    def process(self, element):
        self.element_counter.inc()

        (vivienda_id, data) = element
        personas = data['personas']
        vivienda = data['vivienda'][0] if data['vivienda'] else {}

        for persona in personas:
            persona['vivienda'] = vivienda
            yield persona


class JoinPersonaHogar(beam.DoFn):
    def __init__(self):
        self.element_counter = beam.metrics.Metrics.counter(
            'join_persona_hogar', 'processed_elements')

    def process(self, element):
        self.element_counter.inc()

        (hogar_id, data) = element
        personas = data['personas']
        hogar = data['hogar'][0] if data['hogar'] else {}

        for persona in personas:
            persona['hogar'] = hogar
            yield persona


class JoinViviendaHogar(beam.DoFn):
    def __init__(self):
        self.element_counter = beam.metrics.Metrics.counter(
            'join_vivienda_hogar', 'processed_elements')

    def process(self, element):
        self.element_counter.inc()

        (vivienda_id, data) = element
        hogares = data['hogares']
        vivienda = data['viviendas'][0] if data['viviendas'] else {}

        for hogar in hogares:
            hogar['vivienda'] = vivienda
            yield hogar


options = PipelineOptions(direct_num_workers=0)
with beam.Pipeline(options=options) as pipeline:

    # Lee las entradas
    personas = (
        pipeline
        | 'Leer Personas' >> beam.io.ReadFromParquet('input/personas_censo2024.parquet')
        | 'Limitar Personas' >> beam.Filter(lambda x: random.random() < 0.01)
    )

    viviendas = (
        pipeline
        | 'Leer Viviendas' >> beam.io.ReadFromParquet('input/viviendas_censo2024.parquet')
        | 'Limitar Viviendas' >> beam.Filter(lambda x: random.random() < 0.01)
    )

    hogares = (
        pipeline
        | 'Leer Hogares' >> beam.io.ReadFromParquet('input/hogares_censo2024.parquet')
        | 'Limitar Hogares' >> beam.Filter(lambda x: random.random() < 0.01)
    )

    # Agrupa por llaves
    # En el caso de persona primero agrupa por hogar
    personas_map = (
        personas | 'Mapear Personas a KV' >> beam.Map(
            lambda p: ((p['id_vivienda'], p['id_hogar']), p))
    )

    viviendas_map = (
        viviendas
        | 'Mapear Viviendas a KV' >> beam.Map(lambda v: (v['id_vivienda'], v))
    )

    hogares_map = (
        hogares
        | 'Mapear Hogares a KV' >> beam.Map(lambda h: (h['id_vivienda'], h))
    )

    # Cruza hogares con viviendas
    viviendas_hogar = (
        {'viviendas': viviendas_map, 'hogares': hogares_map}
        | 'Une Viviendas con Hogares' >> beam.CoGroupByKey()
        | 'Viviendas con Hogares' >> beam.ParDo(JoinViviendaHogar())
        | 'Mapea Viviendas con Hogares a KV' >> beam.Map(
            lambda vh: ((vh['id_vivienda'], vh['id_hogar']), vh))
    )

    # Cruza hogares con personas
    personas_hogar = (
        {'personas': personas_map, 'hogares': viviendas_hogar}
        | 'Une Personas con Viviendas y Hogares' >> beam.CoGroupByKey()
        | 'Personas con Viviendas y Hogares' >> beam.ParDo(JoinPersonaHogar())
    )

    # Escribe la salida
    viviendas_hogar | 'Escribir Viviendas con Hogares' >> beam.io.WriteToText(
        "output/viviendas_hogares_enriquecidas.csv")

    # Cruza personas con Hogares
    # personas_hogar = (
    #    {'personas': personas_map, 'hogar': hogares_map}
    #    | 'Une Personas con Hogar' >> beam.CoGroupByKey()
    #    | 'Personas con Hogar' >> beam.ParDo(JoinPersonaHogar())
    # )
#
    # Escribe la salida
    # personas_hogar | 'Escribir Personas Enriquecidas' >> beam.io.WriteToText(
    #    "output/personas_enriquecidas.csv")

    # Cruza personas con viviendas
    # personas_vivienda_group = (
    #    {'personas': personas_by_vivienda_map, 'viviendas': viviendas_map}
    #    | 'Une Personas con Viviendas' >> beam.CoGroupByKey()
    #    | 'Personas con Viviendas' >> beam.ParDo(JoinPersonaVivienda())
    # )
#
    # Mapea personas con vivienda a key-value por Hogar
    # personas_con_vivienda_map = (
    #    personas_vivienda_group
    #    | 'Mapear Personas con Vivienda a KV' >> beam.Map(
    #        lambda p: (p['id_hogar'], p))
    # )
#
    # Cruza personas con viviendas con hogares
    # personas_hogar_group = (
    #    {'personas': personas_con_vivienda_map, 'hogares': hogares_map}
    #    | 'Une Personas con Hogares' >> beam.CoGroupByKey()
    #    | 'Personas con Hogares' >> beam.ParDo(JoinPersonaHogar())
    # )
#
    # Escribe la salida
    # personas_hogar_group | 'Escribir Personas Enriquecidas' >> beam.io.WriteToText(
    #    "output/personas_enriquecidas.csv")
