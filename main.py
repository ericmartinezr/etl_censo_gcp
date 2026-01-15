import json
from typing import Iterable
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromCsv, ReadFromParquet, WriteToText
from apache_beam.pvalue import TaggedOutput
from apache_beam.metrics.metric import Metrics
from apache_beam.pvalue import AsDict
from dotenv import load_dotenv
import time
load_dotenv()


class JoinViviendaHogar(beam.DoFn):
    def __init__(self):
        self.element_counter = Metrics.counter("contador", "JoinViviendaHogar")

    def process(self, element) -> Iterable[TaggedOutput]:
        self.element_counter.inc()

        _, data = element
        hogar = data["hogares"][0] if data["hogares"] else {}
        vivienda = data["viviendas"][0] if data["viviendas"] else {}

        # Si "id_vivienda" existe en hogar, entonces es un objeto correcto
        # En cualquier otro caso no lo es
        if "id_vivienda" in hogar:
            data = (f"{hogar['id_vivienda']}|{hogar['id_hogar']}", {
                **hogar,
                "vivienda": vivienda
            })
            yield TaggedOutput("ok", data)
        else:
            yield TaggedOutput("nok", element)


class JoinHogarViviendaPersona(beam.DoFn):
    def __init__(self):
        self.element_counter = Metrics.counter(
            "contador", "JoinHogarViviendaPersona")

    def process(self, element) -> Iterable[TaggedOutput]:
        self.element_counter.inc()

        _, data = element
        personas = data["personas"] if data["personas"] else {}
        hogar_vivienda = data["hogar_vivienda"][0] if data["hogar_vivienda"] else {
        }

        # Solo personas, independiente si esta asociado con hogar o vivienda
        for persona in personas:
            if persona:
                yield TaggedOutput("ok", {**persona, "hogar": hogar_vivienda})
            else:
                yield TaggedOutput("nok", element)


class MapCodigos(beam.DoFn):
    def __init__(self):
        self.element_counter = Metrics.counter(
            "contador", "MapCodigos")

    def process(self, element: dict, codigos_territorios, codigos_otros, key) -> Iterable[tuple[str, dict]]:
        region = codigos_territorios.get(
            str(element["region"]), {"Territorio": "S/I"})
        provincia = codigos_territorios.get(str(element["provincia"]), {
            "Territorio": "S/I"})
        comuna = codigos_territorios.get(
            str(element["comuna"]), {"Territorio": "S/I"})

        territorio_data = {
            "region": region._asdict()["Territorio"],
            "provincia": provincia._asdict()["Territorio"],
            "comuna": comuna._asdict()["Territorio"]
        }

        map_key = "|".join([str(element[k]) for k in key])
        for campo in element.keys():
            campo_key = f"{campo}|{element[campo]}"
            if campo_key in codigos_otros:
                codigo_info = codigos_otros[campo_key]
                element[campo] = codigo_info._asdict()["Descripcion"]

        yield (map_key, {**element, "ubicacion": territorio_data})


class CleanValores(beam.DoFn):
    def __init__(self):
        self.element_counter = Metrics.counter("contador", "CleanValores")

    def process(self, element: dict) -> Iterable[dict]:
        edad = element["edad"]
        if edad == "Valor suprimido por anonimización":
            edad = -1
        else:
            edad = int(element["edad"])
        element["edad"] = edad

        # Elimina campos innecesarios de la estructura final
        element.pop("region", None)
        element.pop("provincia", None)
        element.pop("comuna", None)

        if "hogar" in element:
            element["hogar"].pop("region", None)
            element["hogar"].pop("provincia", None)
            element["hogar"].pop("comuna", None)
            element["hogar"].pop("id_hogar", None)
            element["hogar"].pop("id_vivienda", None)

            if "vivienda" in element["hogar"]:
                element["hogar"]["vivienda"].pop("region", None)
                element["hogar"]["vivienda"].pop("provincia", None)
                element["hogar"]["vivienda"].pop("comuna", None)
                element["hogar"]["vivienda"].pop("id_vivienda", None)

                # cant_hog puede ser "NA", pero esta definido como campo numeric
                # por lo tanto dejare como -1, ya que el rango es desde 0 a 26
                cant_hog = element["hogar"]["vivienda"]["cant_hog"]
                cant_hog = -1 if not cant_hog else cant_hog
                cant_per = element["hogar"]["vivienda"]["cant_per"]
                element["hogar"]["vivienda"]["cant_per"] = int(cant_per)
                element["hogar"]["vivienda"]["cant_hog"] = int(cant_hog)

        yield element


def run():
    options = PipelineOptions(
        # Local
        runner="DirectRunner",
        direct_num_workers=0,
        direct_running_mode="multi_processing"
    )

    with beam.Pipeline(options=options) as p:

        # Lee entradas de codigos
        codigos_territoriales = (p
                                 | "ReadCodigosTerritorios" >> ReadFromCsv("input/codigos_territoriales.csv",
                                                                           sep=",",
                                                                           names=["Codigo", "Division", "Territorio"])
                                 | "MapCodigosTerritoriosToKV" >> beam.Map(lambda x: (x[0], x))
                                 )

        codigos_otros = (p
                         | "ReadCodigosOtros" >> ReadFromCsv("input/codigos_otros.csv",
                                                             sep=";",
                                                             names=["Campo", "Codigo", "Descripcion"])
                         | "MapCodigosOtrosToKV" >> beam.Map(lambda x: (f"{x[0]}|{x[1]}", x))
                         )

        # Lee las entradas
        viviendas = (p
                     | "ReadViviendas" >> ReadFromParquet("input/viviendas_censo2024.parquet",
                                                          columns=["id_vivienda", "region", "provincia", "comuna", "tipo_operativo",
                                                                   "p2_tipo_vivienda", "p3a_estado_ocupacion", "p3b_estado_ocupacion",
                                                                   "p6_fuente_agua", "p8_serv_hig", "p10_basura",
                                                                   "cant_per", "cant_hog", "indice_hacinamiento"
                                                                   ])
                     # TODO: Borrar, esto es para pruebas
                     | "FilterViviendas" >> beam.Filter(lambda v: v["tipo_operativo"] == 2)
                     )

        hogares = (p
                   | "ReadHogares" >> ReadFromParquet("input/hogares_censo2024.parquet",
                                                      columns=["id_hogar", "id_vivienda", "region", "provincia", "comuna", "tipo_operativo",
                                                               "num_hogar",
                                                               "p12_tenencia_viv", "p13_comb_cocina",
                                                               "p14_comb_calefaccion", "p15a_serv_tel_movil", "p15b_serv_compu",
                                                               "p15d_serv_internet_fija", "p15e_serv_internet_movil", "tipologia_hogar"])

                   # TODO: Borrar, esto es para pruebas
                   | "FilterHogares" >> beam.Filter(lambda v: v["tipo_operativo"] == 2)
                   )

        personas = (p
                    | "ReadPersonas" >> ReadFromParquet("input/personas_censo2024.parquet",
                                                        columns=["id_vivienda", "id_hogar", "id_persona", "region", "provincia", "comuna", "tipo_operativo",
                                                                 "parentesco", "sexo", "edad", "p23_est_civil", "p25_lug_nacimiento_rec", "p27_nacionalidad",
                                                                 "p31_religion", "p37_alfabet", "depend_econ_deficit_hab"])

                    # TODO: Borrar, esto es para pruebas
                    | "FilterPersonas" >> beam.Filter(lambda v: v["tipo_operativo"] == 2)
                    )

        # Convierte codigos a valores segun los diccionarios
        viviendas_map = viviendas | "MapCodigosToVivienda" >> beam.ParDo(
            MapCodigos(),
            AsDict(codigos_territoriales),
            AsDict(codigos_otros),
            ["id_vivienda"])

        hogares_map = hogares | "MapCodigosToHogar" >> beam.ParDo(
            MapCodigos(),
            AsDict(codigos_territoriales),
            AsDict(codigos_otros),
            ["id_vivienda"])

        persona_map = personas | "MapCodigosToPersona" >> beam.ParDo(
            MapCodigos(),
            AsDict(codigos_territoriales),
            AsDict(codigos_otros),
            ["id_vivienda", "id_hogar"])

        # Join entre hogar y vivienda
        join_ok, join_nok = (
            {"viviendas": viviendas_map, "hogares": hogares_map}
            | "JoinHogarVivienda" >> beam.CoGroupByKey()
            | "MapHogarViviendaToKV" >> beam.ParDo(JoinViviendaHogar()).with_outputs("ok", "nok")
        )

        # Join entre hogar_vivienda y persona
        final, error = (
            {"hogar_vivienda": join_ok, "personas": persona_map}
            | "JoinHogarViviendaPersona" >> beam.CoGroupByKey()
            | "MapHogarViviendaPersona" >> beam.ParDo(JoinHogarViviendaPersona()).with_outputs("ok", "nok")
        )

        (final
         | "CleanValores" >> beam.ParDo(CleanValores())
         | "WriteHogarViviendaPersonaToFile" >> WriteToText(
             "output/final.jsonl"))

        join_nok | "WriteJoinFail" >> WriteToText(
            "output/join_fail.jsonl")

        error | "WriteFinaJoinFail" >> WriteToText(
            "output/final_fail.jsonl")


if __name__ == "__main__":
    run()
