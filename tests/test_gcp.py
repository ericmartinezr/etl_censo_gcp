import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.io import ReadFromCsv
from apache_beam.pvalue import AsDict
from gcp import MapCodigos, JoinViviendaHogar, JoinHogarViviendaPersona, CleanValores

COD_TERRITORIALES = {104: {"Codigo": 104,
                           "Division": "Provincia", "Territorio": "Palena"}}
COD_OTROS = {"area": {1: "Urbano", 2: "Rural"}}

VIVIENDA = [
    {
        "id_vivienda": 343446,
        "region": 13,
        "provincia": 131,
        "comuna": 13127,
        "tipo_operativo": 2,
        "p2_tipo_vivienda": 1,
        "p3a_estado_ocupacion": 1,
        "p3b_estado_ocupacion": 1,
        "p6_fuente_agua": 1,
        "p8_serv_hig": 1,
        "p10_basura": 1,
        "cant_per": 1,
        "cant_hog": 1,
        "indice_hacinamiento": 1
    }
]

VIVIENDA_MAP_CODIGO = [("343446", {
    "id_vivienda": 343446,
    "region": 13,
    "provincia": 131,
    "comuna": 13127,
    "tipo_operativo": "Vivienda particular",
    "p2_tipo_vivienda": "Casa",
    "p3a_estado_ocupacion": "Ocupada",
    "p3b_estado_ocupacion": "Moradores presentes",
    "p6_fuente_agua": "Red pública",
    "p8_serv_hig": "Dentro de la vivienda, conectado a una red de alcantarillado",
    "p10_basura": "La recogen los servicios de aseo",
    "cant_per": 1,
    "cant_hog": 1,
    "indice_hacinamiento": "Vivienda sin hacinamiento",
    "ubicacion": {
        "region": "Metropolitana de Santiago",
        "provincia": "Santiago",
        "comuna": "Recoleta"
    }
})]

HOGAR = [
    {
        "id_hogar": 1,
        "id_vivienda": 343446,
        "region": 13,
        "provincia": 131,
        "comuna": 13127,
        "tipo_operativo": 2,
        "p12_tenencia_viv": 9,
        "p13_comb_cocina": 1,
        "p14_comb_calefaccion": 1,
        "p15a_serv_tel_movil": 1,
        "p15b_serv_compu": 2,
        "p15d_serv_internet_fija": 1,
        "p15e_serv_internet_movil": 1,
        "tipologia_hogar": 1
    }
]

HOGAR_MAP_CODIGO = [("343446", {
    "id_hogar": 1,
    "id_vivienda": 343446,
    "region": 13,
    "provincia": 131,
    "comuna": 13127,
    "tipo_operativo": "Vivienda particular",
    "p12_tenencia_viv": "Propiedad en sucesión o litigio",
    "p13_comb_cocina": "Gas",
    "p14_comb_calefaccion": "Gas",
    "p15a_serv_tel_movil": "Sí",
    "p15b_serv_compu": "No",
    "p15d_serv_internet_fija": "Sí",
    "p15e_serv_internet_movil": "Sí",
    "tipologia_hogar": "Unipersonal",
    "ubicacion": {
        "region": "Metropolitana de Santiago",
        "provincia": "Santiago",
        "comuna": "Recoleta"
    }
})]

PERSONA = [
    {
        "id_vivienda": 343446,
        "id_hogar": 1,
        "id_persona": 1,
        "region": 13,
        "provincia": 131,
        "comuna": 13127,
        "tipo_operativo": 2,
        "parentesco": 1,
        "sexo": 2,
        "edad": 58,
        "p23_est_civil": 6,
        "p25_lug_nacimiento_rec": 1,
        "p27_nacionalidad": 1,
        "p31_religion": 1,
        "p37_alfabet": 1,
        "depend_econ_deficit_hab": 2
    }
]

PERSONA_MAP_CODIGO = [("343446", {
    "id_hogar": 1,
    "id_vivienda": 343446,
    "region": 13,
    "provincia": 131,
    "comuna": 13127,
    "tipo_operativo": "Vivienda particular",
    "p12_tenencia_viv": "Propiedad en sucesión o litigio",
    "p13_comb_cocina": "Gas",
    "p14_comb_calefaccion": "Gas",
    "p15a_serv_tel_movil": "Sí",
    "p15b_serv_compu": "No",
    "p15d_serv_internet_fija": "Sí",
    "p15e_serv_internet_movil": "Sí",
    "tipologia_hogar": "Unipersonal",
    "ubicacion": {
        "region": "Metropolitana de Santiago",
        "provincia": "Santiago",
        "comuna": "Recoleta"
    }
})]

JOIN_VIVIENDA_HOGAR = [("343446|1", {
    "id_vivienda": 343446,
    "id_hogar": 1,
    "id_persona": 1,
    "region": 13,
    "provincia": 131,
    "comuna": 13127,
    "tipo_operativo": "Vivienda particular",
    "parentesco": "Jefe/a de hogar",
    "sexo": "Mujer",
    "edad": 58,
    "p23_est_civil": "Divorciado/a",
    "p25_lug_nacimiento_rec": "Sí",
    "p27_nacionalidad": "Chilena (exclusivamente)",
    "p31_religion": "Católica",
    "p37_alfabet": "Sí",
    "depend_econ_deficit_hab": "Económicamente dependiente",
    "ubicacion": {
            "region": "Metropolitana de Santiago",
            "provincia": "Santiago",
            "comuna": "Recoleta"
    }
})]


class GCPTest(unittest.TestCase):

    def test_pipeline(self):
        with TestPipeline() as p:

            # Archivos con codigos
            codigos_territoriales = (p
                                     | "ReadCodigosTerritorio" >> ReadFromCsv(f"input/codigos_territoriales.csv",
                                                                              sep=",",
                                                                              names=["Codigo", "Division", "Territorio"])
                                     | "MapCodigosTerritoriosToKV" >> beam.Map(lambda x: (x[0], x))
                                     )

            codigos_otros = (p
                             | "ReadCodigosOtros" >> ReadFromCsv(f"input/codigos_otros.csv",
                                                                 sep=";",
                                                                 names=["Campo", "Codigo", "Descripcion"])
                             | "MapCodigosOtrosToKV" >> beam.Map(lambda x: (f"{x[0]}|{x[1]}", x))
                             )

            # Prueba de Vivienda
            vivienda = p | "CreateVivienda" >> beam.Create(VIVIENDA)
            output_vivienda = vivienda | "MapCodigosToVivienda" >> beam.ParDo(
                MapCodigos(),
                AsDict(codigos_territoriales),
                AsDict(codigos_otros),
                ["id_vivienda"])

            # Prueba de Hogar
            hogar = p | "CreateHogar" >> beam.Create(HOGAR)
            output_hogar = hogar | "MapCodigosToHogar" >> beam.ParDo(
                MapCodigos(),
                AsDict(codigos_territoriales),
                AsDict(codigos_otros),
                ["id_vivienda"])

            # Pruebad de Persona
            persona = p | "CreatePersona" >> beam.Create(PERSONA)
            output_persona = persona | "MapCodigosToPersona" >> beam.ParDo(
                MapCodigos(),
                AsDict(codigos_territoriales),
                AsDict(codigos_otros),
                ["id_vivienda", "id_hogar"])

            # Join entre hogar y vivienda
            join_vh, join_vh_error = (
                {"viviendas": output_vivienda, "hogares": output_hogar}
                | "JoinHogarVivienda" >> beam.CoGroupByKey()
                | "MapHogarViviendaToKV" >> beam.ParDo(JoinViviendaHogar()).with_outputs("ok", "nok")
            )

            # Join entre hogar_vivienda y persona
            # final, error = (
            #    {"hogar_vivienda": join_vh, "personas": persona}
            #    | "JoinHogarViviendaPersona" >> beam.CoGroupByKey()
            #    | "MapHogarViviendaPersona" >> beam.ParDo(JoinHogarViviendaPersona()).with_outputs("ok", "nok")
            # )
#
            # result = final | "CleanValores" >> beam.ParDo(CleanValores())

            assert_that(output_vivienda, equal_to(VIVIENDA_MAP_CODIGO))
            assert_that(output_hogar, equal_to(HOGAR_MAP_CODIGO))
            assert_that(output_persona, equal_to(PERSONA_MAP_CODIGO))
            assert_that(join_vh, equal_to(JOIN_VIVIENDA_HOGAR))
#            assert_that(join_vh_error, equal_to([]))


if __name__ == "__main__":
    unittest.main()
