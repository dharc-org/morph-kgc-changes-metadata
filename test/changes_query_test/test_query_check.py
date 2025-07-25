import unittest
from rdflib import Graph


class TestMergedGraphQueries(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Percorsi ai file TTL
        ttl_file1 = "test/changes_query_test/ttl_test_dataset/knowledge-graph_obj_corretto.ttl"
        ttl_file2 = "test/changes_query_test/ttl_test_dataset/knowledge-graph_pro_corretto.ttl"

        cls.graph1 = Graph().parse(ttl_file1, format="turtle")
        cls.graph2 = Graph().parse(ttl_file2, format="turtle")

        cls.merged_graph = Graph()
        for t in cls.graph1: cls.merged_graph.add(t)
        for t in cls.graph2: cls.merged_graph.add(t)

        # Bind both possible Getty patterns so either one resolves in queries
        cls.merged_graph.bind("aat",  "http://vocab.getty.edu/aat/")
        cls.merged_graph.bind("aatp", "http://vocab.getty.edu/page/aat/")

        # copia altri namespace dal dataset
        for p,u in set(cls.graph1.namespaces())|set(cls.graph2.namespaces()):
            cls.merged_graph.bind(p,u)

        cls.merged_graph.serialize(
            destination="test/changes_query_test/unified_graph/merged_graph_output.ttl",
            format="turtle"
        )


    # ---------- helper ----------
    def run_query_and_assert(self, query: str, description: str):
        """
        Runs the SPARQL query on the merged graph and checks that
        at least one binding is returned.  If 0 bindings are found,
        the helper tries once more replacing the Getty prefix
        'http://vocab.getty.edu/page/aat/' with 'http://vocab.getty.edu/aat/'.
        """
        def _exec(q):
            try:
                return list(self.merged_graph.query(q))
            except Exception as e:
                self.fail(f"Query ({description}) raised: {e}")

        results = _exec(query)

        # auto-retry with alternate Getty prefix if nothing returned
        if not results and "http://vocab.getty.edu/page/aat/" in query:
            alt_query = query.replace(
                "http://vocab.getty.edu/page/aat/",
                "http://vocab.getty.edu/aat/")
            results = _exec(alt_query)

        self.assertTrue(results, f"Query ({description}) produced 0 rows.")

        row0 = results[0]
        first_binding = (
            row0.asdict() if hasattr(row0, "asdict")
            else {k: v for k, v in row0.binding.items()}
        )
        print(f"{description} ✓ first binding → {first_binding}")

    def test_titolo_museale(self):
        query = """
        PREFIX aat: <http://vocab.getty.edu/page/aat/>
        PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
        PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>

        SELECT ?item ?content WHERE {
            ?item a lrmoo:F5_Item .
            ?manifestation lrmoo:R7i_is_exemplified_by ?item .
            ?expression lrmoo:R4i_is_embodied_in ?manifestation .
            ?work lrmoo:R3_is_realised_in ?expression ;
                  crm:P102_has_title ?title .
            ?title crm:P2_has_type aat:300417207 ;
                   crm:P190_has_symbolic_content ?content .
            FILTER(lang(?content) = "it")
        }
        """
        self.run_query_and_assert(query, "Titolo Museale")

    def test_tipologia(self):
        query = """
        PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
        PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>

        SELECT ?item ?type WHERE {
            ?item a lrmoo:F5_Item .
            ?manifestation lrmoo:R7i_is_exemplified_by ?item ;
                          crm:P2_has_type ?type .
        }
        """
        self.run_query_and_assert(query, "Tipologia")

    def test_didascalia(self):
        query = """
        PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
        PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>

        SELECT ?item ?description WHERE {
            ?item a lrmoo:F5_Item ;
                  crm:P3_has_note ?description .
        }
        """
        self.run_query_and_assert(query, "Didascalia")

    def test_tecnica(self):
        query = """
        PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
        PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>

        SELECT ?item ?technique WHERE {
            ?item a lrmoo:F5_Item .
            ?manifestation lrmoo:R7i_is_exemplified_by ?item .
            ?expression lrmoo:R4i_is_embodied_in ?manifestation .
            ?creation a lrmoo:F28_Expression_Creation ;
                      lrmoo:R17_created ?expression ;
                      crm:P32_used_general_technique ?technique .
        }
        """
        self.run_query_and_assert(query, "Tecnica")

    def test_data(self):
        query = """
        PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
        PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>

        SELECT ?item (CONCAT(
             STRDAY(?begin), "/", STRMONTH(?begin), "/", STRYEAR(?begin),
             "-",
             STRDAY(?end), "/", STRMONTH(?end), "/", STRYEAR(?end)
        ) AS ?date)
        WHERE {
            ?item a lrmoo:F5_Item .
            ?manifestation lrmoo:R7i_is_exemplified_by ?item .
            ?expression lrmoo:R4i_is_embodied_in ?manifestation .
            ?creation a lrmoo:F28_Expression_Creation ;
                      lrmoo:R17_created ?expression ;
                      crm:P4_has_time-span ?timespan .
            ?timespan crm:P82a_begin_of_the_begin ?begin ;
                      crm:P82b_end_of_the_end ?end .
        }
        """
        self.run_query_and_assert(query, "Data")

    def test_ente_conservazione(self):
        query = """
        PREFIX aat: <http://vocab.getty.edu/page/aat/>
        PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
        PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>

        SELECT ?item ?conservation_org WHERE {
            ?item a lrmoo:F5_Item .
            ?activity crm:P16_used_specific_object ?item ;
                      crm:P2_has_type aat:300054277 ;
                      crm:P14_carried_out_by ?conservation_org .
        }
        """
        self.run_query_and_assert(query, "Ente di Conservazione")

    def test_luogo_conservazione(self):
        query = """
        PREFIX aat: <http://vocab.getty.edu/page/aat/>
        PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
        PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>

        SELECT ?item ?conservation_place WHERE {
            ?item a lrmoo:F5_Item .
            ?activity crm:P16_used_specific_object ?item ;
                      crm:P2_has_type aat:300054277 ;
                      crm:P14_carried_out_by ?conservation_org .
            ?conservation_org crm:P74_has_current_or_former_residence ?conservation_place .
        }
        """
        self.run_query_and_assert(query, "Luogo di Conservazione")

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
        GROUP BY ?item ?license_link
        """
        self.run_query_and_assert(query, "Licenza Modello 3D")

    def test_riproduzione_digitale(self):
        query = """
        PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
        PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>

        SELECT ?item ?digital_repr WHERE {
            ?item a lrmoo:F5_Item .
            ?manifestation lrmoo:R7i_is_exemplified_by ?item ;
                          crm:P130i_features_are_also_found_on ?digital_repr .
        }
        """
        self.run_query_and_assert(query, "Riproduzione Digitale")

    def test_modello_digitale_esistente(self):
        query = """
        PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
        PREFIX crmdig: <http://www.ics.forth.gr/isl/CRMdig/>
        PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>

        SELECT ?item ?existing_model WHERE {
            ?item a lrmoo:F5_Item ;
                  crm:P130i_features_are_also_found_on ?3d_model .
            ?3d_model a crmdig:D9_Data_Object ;
                      crm:P130_shows_features_of ?existing_model .
        }
        """
        self.run_query_and_assert(query, "Modello Digitale Esistente")

    def test_soggetti(self):
        query = """
        PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
        PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>

        SELECT ?item (GROUP_CONCAT(REPLACE(REPLACE(STR(?subject), '.*/([^/]+)/[^/]+', '$1'), '_', ' '); SEPARATOR='; ') AS ?subjects) WHERE {
            ?item a lrmoo:F5_Item .
            ?manifestation lrmoo:R7i_is_exemplified_by ?item .
            ?expression lrmoo:R4i_is_embodied_in ?manifestation ;
                        crm:P129_is_about ?subject .
        } GROUP BY ?item
        """
        self.run_query_and_assert(query, "Soggetti")

    def test_persone_enti(self):
        query = """
        PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
        PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>

        SELECT ?item (GROUP_CONCAT(CONCAT(?clean_name, " (", STR(?activity_type), ")"); SEPARATOR='; ') AS ?agents) WHERE {
            ?item a lrmoo:F5_Item .
            ?manifestation lrmoo:R7i_is_exemplified_by ?item .
            ?expression lrmoo:R4i_is_embodied_in ?manifestation .
            ?creation a lrmoo:F28_Expression_Creation ; lrmoo:R17_created ?expression ; crm:P9_consists_of ?activity .
            ?activity crm:P2_has_type ?activity_type ; crm:P14_carried_out_by ?agent .
            BIND(STRAFTER(STR(?agent), "acr/") AS ?after_acr)
            BIND(STRBEFORE(?after_acr, "/1") AS ?agent_name_raw)
            BIND(REPLACE(?agent_name_raw, "_", " ") AS ?clean_name)
        } GROUP BY ?item
        """
        self.run_query_and_assert(query, "Persone o Enti")

    def test_link_ad_aton(self):
        query = """
        PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
        PREFIX crmdig: <http://www.ics.forth.gr/isl/CRMdig/>
        PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>

        SELECT ?item ?link_aton WHERE {
            ?item a lrmoo:F5_Item .
            ?acquisition crmdig:L1_digitized ?item ; crmdig:L11_had_output ?3d_model_00 .
            ?processing crmdig:L10_had_input ?3d_model_00 ; crmdig:L11_had_output ?3d_model_01 .
            ?modeling crmdig:L10_had_input ?3d_model_01 ; crmdig:L11_had_output ?3d_model_02 .
            ?optimization crmdig:L10_had_input ?3d_model_02 ; crmdig:L11_had_output ?3d_model_03 .
            ?exporting crmdig:L10_had_input ?3d_model_03 ; crmdig:L11_had_output ?3d_model_04 .
            ?metadata crmdig:L10_had_input ?3d_model_04 ; crmdig:L11_had_output ?3d_model_05 .
            ?publication crmdig:L10_had_input ?3d_model_05 ; crmdig:L11_had_output ?3d_model_06 .
            ?3d_model_06 crm:P1_is_identified_by ?id_aton .
            ?id_aton crm:P190_has_symbolic_content ?link_aton .
        } GROUP BY ?link_aton
        """
        self.run_query_and_assert(query, "Link ad Aton")

    def test_dati_giocattolo_utilizzati(self):
        query = """
        PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>

        SELECT ?item ?property ?value WHERE {
            ?item a lrmoo:F5_Item .
            ?item ?property ?value .
        }
        """
        self.run_query_and_assert(query, "Dati Giocattolo Utilizzati")


if __name__ == '__main__':
    unittest.main()

# (morph-kgc-changes-metadata-conversions-py3.12) (base) ariannamorettj@MacBook-Air-di-arianna morph-kgc-changes-metadata-dharc % python -m unittest test.changes_query_test.test_query_check -v