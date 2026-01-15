import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.io import ReadFromCsv
from apache_beam.pvalue import AsDict
from gcp import MapCodigos, JoinViviendaHogar, JoinHogarViviendaPersona, CleanValores
from tests.data import *


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
            final, error = (
                {"hogar_vivienda": join_vh, "personas": output_persona}
                | "JoinHogarViviendaPersona" >> beam.CoGroupByKey()
                | "MapHogarViviendaPersona" >> beam.ParDo(JoinHogarViviendaPersona()).with_outputs("ok", "nok")
            )

            result = final | "CleanValores" >> beam.ParDo(CleanValores())

            assert_that(output_vivienda, equal_to(VIVIENDA_MAP_CODIGO))
            assert_that(output_hogar, equal_to(HOGAR_MAP_CODIGO))
            assert_that(output_persona, equal_to(PERSONA_MAP_CODIGO))
            assert_that(join_vh, equal_to(JOIN_VIVIENDA_HOGAR))
            assert_that(join_vh_error, equal_to([]))
            assert_that(final, equal_to(JOIN_HOGAR_PERSONA))
            assert_that(error, equal_to([]))
            assert_that(result, equal_to(RESULT))


if __name__ == "__main__":
    unittest.main()
