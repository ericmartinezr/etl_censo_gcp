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

PERSONA_MAP_CODIGO = [("343446|1", {
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

JOIN_VIVIENDA_HOGAR = [("343446|1", {
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
    },
    "vivienda": {
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
    }
})]

JOIN_HOGAR_PERSONA = [{
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
    },
    "hogar": {
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
        },
        "vivienda": {
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
        }
    }
}
]

RESULT = [{
    "id_vivienda": 343446,
    "id_hogar": 1,
    "id_persona": 1,
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
    },
    "hogar": {
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
        },
        "vivienda": {
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
        }
    }
}
]
