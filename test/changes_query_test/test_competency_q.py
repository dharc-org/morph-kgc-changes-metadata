import unittest
from rdflib import Graph


class TestMergedGraphQueries(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Percorsi ai file TTL
        ttl_file1 = "test/changes_query_test/ttl_test_dataset/knowledge-graph_obj_corretto.ttl"
        ttl_file2 = "test/changes_query_test/ttl_test_dataset/knowledge-graph_pro_corretto.ttl"

        # Carica i grafi separati
        cls.graph1 = Graph()
        cls.graph1.parse(ttl_file1, format="turtle")
        cls.graph2 = Graph()
        cls.graph2.parse(ttl_file2, format="turtle")

        # Unisci i grafi in uno solo
        cls.merged_graph = Graph()
        for triple in cls.graph1:
            cls.merged_graph.add(triple)
        for triple in cls.graph2:
            cls.merged_graph.add(triple)

        # Mantieni i namespace dei grafi originali
        for prefix, uri in set(cls.graph1.namespaces()) | set(cls.graph2.namespaces()):
            cls.merged_graph.bind(prefix, uri)

        # Serializza il grafo unito su file per eventuali controlli esterni
        cls.merged_graph.serialize(
            destination="test/changes_query_test/unified_graph/merged_graph_output.ttl",
            format="turtle"
        )

    def run_query_and_assert(self, query: str, description: str):
        """
        Esegue la query SPARQL sul grafo unito e verifica che restituisca almeno un risultato.
        PASS se c’è almeno un binding (stampa il primo);
        FAIL se solleva eccezione o non ritorna binding.
        """
        try:
            results = list(self.merged_graph.query(query))
        except Exception as e:
            self.fail(f"Query ({description}) ha sollevato un'eccezione: {e}")
        self.assertTrue(
            results,
            f"Query ({description}) non ha restituito risultati."
        )
        # Estrai primo binding come dict
        first = results[0]
        binding = {var: first[var] for var in getattr(first, 'labels', [])}
        print(f"{description} PASSED: primo binding = {binding}")

    # def test_titolo_museale(self):
    #     query = """
    #     PREFIX aat: <http://vocab.getty.edu/page/aat/>
    #     PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
    #     PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>
    #
    #     SELECT ?item ?content WHERE {
    #         ?item a lrmoo:F5_Item .
    #         ?manifestation lrmoo:R7i_is_exemplified_by ?item .
    #         ?expression lrmoo:R4i_is_embodied_in ?manifestation .
    #         ?work lrmoo:R3_is_realised_in ?expression ;
    #               crm:P102_has_title ?title .
    #         ?title crm:P2_has_type aat:300417207 ;
    #                crm:P190_has_symbolic_content ?content .
    #         FILTER(lang(?content) = "it")
    #     }
    #     """
    #     self.run_query_and_assert(query, "Titolo Museale")

    # def test_tipologia(self):
    #     query = """
    #     PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
    #     PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>
    #
    #     SELECT ?item ?type WHERE {
    #         ?item a lrmoo:F5_Item .
    #         ?manifestation lrmoo:R7i_is_exemplified_by ?item ;
    #                       crm:P2_has_type ?type .
    #     }
    #     """
    #     self.run_query_and_assert(query, "Tipologia")

    # def test_didascalia(self):
    #     query = """
    #     PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
    #     PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>
    #
    #     SELECT ?item ?description WHERE {
    #         ?item a lrmoo:F5_Item ;
    #               crm:P3_has_note ?description .
    #     }
    #     """
    #     self.run_query_and_assert(query, "Didascalia")

    # def test_tecnica(self):
    #     query = """
    #     PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
    #     PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>
    #
    #     SELECT ?item ?technique WHERE {
    #         ?item a lrmoo:F5_Item .
    #         ?manifestation lrmoo:R7i_is_exemplified_by ?item .
    #         ?expression lrmoo:R4i_is_embodied_in ?manifestation .
    #         ?creation a lrmoo:F28_Expression_Creation ;
    #                   lrmoo:R17_created ?expression ;
    #                   crm:P32_used_general_technique ?technique .
    #     }
    #     """
    #     self.run_query_and_assert(query, "Tecnica")
    #
    # def test_data(self):
    #     query = """
    #         PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/> PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/> SELECT ?item (IF(STR(YEAR(?begin)) = STR(YEAR(?end)), STR(YEAR(?begin)), CONCAT(STR(YEAR(?begin)), '-', STR(YEAR(?end)))) AS ?date) WHERE { ?item a lrmoo:F5_Item . ?manifestation lrmoo:R7i_is_exemplified_by ?item . ?expression lrmoo:R4i_is_embodied_in ?manifestation . ?creation a lrmoo:F28_Expression_Creation ; lrmoo:R17_created ?expression ; crm:P4_has_time-span ?timespan . ?timespan crm:P82a_begin_of_the_begin ?begin ; crm:P82b_end_of_the_end ?end . }        """
    #     self.run_query_and_assert(query, "Data")

    # def test_ente_conservazione(self):
    #     query = """
    #     PREFIX aat: <http://vocab.getty.edu/page/aat/>
    #     PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
    #     PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>
    #
    #     SELECT ?item ?conservation_org WHERE {
    #         ?item a lrmoo:F5_Item .
    #         ?activity crm:P16_used_specific_object ?item ;
    #                   crm:P2_has_type aat:300054277 ;
    #                   crm:P14_carried_out_by ?conservation_org .
    #     }
    #     """
    #     self.run_query_and_assert(query, "Ente di Conservazione")

    # def test_luogo_conservazione(self):
    #     query = """
    #     PREFIX aat: <http://vocab.getty.edu/page/aat/>
    #     PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
    #     PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>
    #
    #     SELECT ?item ?conservation_place WHERE {
    #         ?item a lrmoo:F5_Item .
    #         ?activity crm:P16_used_specific_object ?item ;
    #                   crm:P2_has_type aat:300054277 ;
    #                   crm:P14_carried_out_by ?conservation_org .
    #         ?conservation_org crm:P74_has_current_or_former_residence ?conservation_place .
    #     }
    #     """
    #     self.run_query_and_assert(query, "Luogo di Conservazione")

    def test_licenza_modello_3d(self):
        query = """
        PREFIX aat: <http://vocab.getty.edu/page/aat/>
        PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
        PREFIX crmdig: <http://www.ics.forth.gr/isl/CRMdig/>
        PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>

        SELECT ?item ?license_link WHERE {
            ?item a lrmoo:F5_Item .
            ?acquisition crmdig:L1_digitized ?item .
            ?acquisition (crmdig:L11_had_output)+ ?3d_model .
            ?license crm:P2_has_type aat:300435434 ;
                     crm:P67_refers_to ?3d_model ;
                     crm:P70i_is_documented_in ?license_link .
        }
        """
        self.run_query_and_assert(query, "Licenza Modello 3D")

    # def test_riproduzione_digitale(self):
    #     query = """
    #     PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
    #     PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>
    #
    #     SELECT ?item ?digital_repr WHERE {
    #         ?item a lrmoo:F5_Item .
    #         ?manifestation lrmoo:R7i_is_exemplified_by ?item ;
    #                       crm:P130i_features_are_also_found_on ?digital_repr .
    #     }
    #     """
    #     self.run_query_and_assert(query, "Riproduzione Digitale")

    # def test_modello_digitale_esistente(self):
    #     query = """
    #     PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
    #     PREFIX crmdig: <http://www.ics.forth.gr/isl/CRMdig/>\n        PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>
    #
    #     SELECT ?item ?existing_model WHERE {
    #         ?item a lrmoo:F5_Item ;
    #               crm:P130i_features_are_also_found_on ?3d_model .
    #         ?3d_model a crmdig:D9_Data_Object ;
    #                   crm:P130_shows_features_of ?existing_model .
    #     }
    #     """
    #     self.run_query_and_assert(query, "Modello Digitale Esistente")
    #
    # def test_soggetti(self):
    #     query = """
    #     PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
    #     PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>
    #
    #     SELECT ?item (GROUP_CONCAT(
    #         REPLACE(REPLACE(STR(?subject), '.*/([^/]+)/[^/]+', '$1'), '_', ' ')
    #         ; SEPARATOR='; '
    #     ) AS ?subjects)
    #     WHERE {
    #         ?item a lrmoo:F5_Item .
    #         ?manifestation lrmoo:R7i_is_exemplified_by ?item .
    #         ?expression lrmoo:R4i_is_embodied_in ?manifestation ;
    #                     crm:P129_is_about ?subject .
    #     }
    #     GROUP BY ?item
    #     """
    #     self.run_query_and_assert(query, "Soggetti")
    #
    # def test_persone_enti(self):
    #     query = """
    #     PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
    #     PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>
    #
    #     SELECT ?item (GROUP_CONCAT(
    #         CONCAT(
    #             STRAFTER(STR(?agent), "acr/"),
    #             " (", STR(?activity_type), ")"
    #         )
    #         ; SEPARATOR='; '
    #     ) AS ?agents)
    #     WHERE {
    #         ?item a lrmoo:F5_Item .
    #         ?manifestation lrmoo:R7i_is_exemplified_by ?item .
    #         ?expression lrmoo:R4i_is_embodied_in ?manifestation .
    #         ?creation a lrmoo:F28_Expression_Creation ;
    #                   lrmoo:R17_created ?expression ;
    #                   crm:P9_consists_of ?activity .
    #         ?activity crm:P2_has_type ?activity_type ;
    #                   crm:P14_carried_out_by ?agent .
    #     }
    #     GROUP BY ?item
    #     """
    #     self.run_query_and_assert(query, "Persone o Enti")
    #
    # def test_link_ad_aton(self):
    #     query = """
    #     PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
    #     PREFIX crmdig: <http://www.ics.forth.gr/isl/CRMdig/>
    #     PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>
    #
    #     SELECT ?item ?link_aton WHERE {
    #         ?item a lrmoo:F5_Item .
    #         OPTIONAL {
    #             ?acquisition crmdig:L1_digitized ?item ;
    #                          crmdig:L11_had_output ?3d_model_00 .
    #         }
    #         OPTIONAL {
    #             ?item crm:P130i_features_are_also_found_on ?3d_model_00 .
    #         }
    #         # chain path to any 3d_model
    #         ?3d_model_00 (^crmdig:L10_had_input/crmdig:L11_had_output)+ ?3d_model .
    #         ?3d_model crm:P1_is_identified_by ?id_aton .
    #         ?id_aton crm:P190_has_symbolic_content ?link_aton .
    #     }
    #     GROUP BY ?item ?link_aton
    #     """
    #     self.run_query_and_assert(query, "Link ad Aton")



if __name__ == '__main__':
    unittest.main()
