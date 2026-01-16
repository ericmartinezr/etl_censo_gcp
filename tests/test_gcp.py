import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.io import ReadFromCsv
from apache_beam.pvalue import AsDict
from gcp import MapCodigos, JoinViviendaHogar, JoinHogarViviendaPersona, CleanValores
from tests.mock_data import *
from tests.mock_codigos_territoriales import CODIGOS_TERRITORIALES
from tests.mock_codigos_otros import CODIGOS_OTROS


class GCPTest(unittest.TestCase):

    def build_test_pipeline(self, p):

        # Archivos con codigos
        codigos_territoriales = (
            p
            | "CreateCodigosTerritoriales" >> beam.Create(CODIGOS_TERRITORIALES)
            | "MapCodigosTerritoriosToKV" >> beam.Map(lambda x: (f"{x[0]}|{x[1]}", x))
        )
        codigos_otros = (
            p
            | "CreateCodigosOtros" >> beam.Create(CODIGOS_OTROS)
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

        return {
            "output_vivienda": output_vivienda,
            "output_hogar": output_hogar,
            "output_persona": output_persona,
            "join_vh": join_vh,
            "join_vh_error": join_vh_error,
            "final": final,
            "error": error
        }

    def test_output_vivienda(self):
        with TestPipeline() as p:
            test_data = self.build_test_pipeline(p)
            assert_that(test_data["output_vivienda"], equal_to(
                VIVIENDA_MAP_CODIGO), label="TestOuputVivienda")

    def test_output_hogar(self):
        with TestPipeline() as p:
            test_data = self.build_test_pipeline(p)
            assert_that(test_data["output_hogar"], equal_to(
                HOGAR_MAP_CODIGO), label="TestOuputHogar")

    def test_output_persona(self):
        with TestPipeline() as p:
            test_data = self.build_test_pipeline(p)
            assert_that(test_data["output_persona"], equal_to(
                PERSONA_MAP_CODIGO), label="TestOutputPersona")

    def test_join_vivienda_hogar(self):
        with TestPipeline() as p:
            test_data = self.build_test_pipeline(p)
            assert_that(test_data["join_vh"], equal_to(
                JOIN_VIVIENDA_HOGAR), label="TestJoinViviendaHogar")

    def test_join_vivienda_hogar_error(self):
        with TestPipeline() as p:
            test_data = self.build_test_pipeline(p)
            assert_that(test_data["join_vh_error"], equal_to(
                []), label="TestJoinViviendaHogarError")

    def test_join_hogar_persona(self):
        with TestPipeline() as p:
            test_data = self.build_test_pipeline(p)
            assert_that(test_data["final"], equal_to(
                JOIN_HOGAR_PERSONA), label="TestJoinHogarPersona")

    def test_join_hogar_persona_error(self):
        with TestPipeline() as p:
            test_data = self.build_test_pipeline(p)
            assert_that(test_data["error"], equal_to([]),
                        label="TestJoinHogarPersonaError")

    # Extiende el pipeline de forma individual
    # porque dejarlo en la funcion build_test_pipeline
    # genera un side effect que altera el valor de "final"
    # antes de que lo vea este test
    def test_final_result(self):
        with TestPipeline() as p:
            test_data = self.build_test_pipeline(p)
            final = test_data["final"]
            result = final | "CleanValores" >> beam.ParDo(
                CleanValores())
            assert_that(result, equal_to(
                RESULT), label="TestFinalResult")


if __name__ == "__main__":
    unittest.main()
