import unittest
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, XSD

# --- rdflib: disabilita cast automatico di xsd:dateTime (e opzionalmente xsd:date) ---
try:
    from rdflib.term import _toPythonMapping
    from rdflib.namespace import XSD
    _toPythonMapping.pop(XSD.dateTime, None)
    _toPythonMapping.pop(XSD.date, None)  # se comparisse xsd:date
    print("[INFO] rdflib datetime cast disabled in this process")
except Exception as e:
    print(f"[WARN] rdflib datetime cast patch skipped: {e}")


class TestGraphQuality(unittest.TestCase):
    unified_graph_path = "results/merged_graph_output.ttl"

    @classmethod
    def setUpClass(cls):
        cls.g = Graph()
        with open(cls.unified_graph_path, "r", encoding="utf-8") as f:
            cls.g.parse(data=f.read(), format="turtle")
        cls.crm = Namespace("http://www.cidoc-crm.org/cidoc-crm/")
        cls.edtf_dt = URIRef("http://id.loc.gov/datatypes/edtf/EDTF")
    def _literal_value(self, subj_iri: str) -> str:
        lit = self.g.value(URIRef(subj_iri), self.crm.P190_has_symbolic_content)
        self.assertIsInstance(lit, Literal)
        return str(lit)

    def test_titles_literals_encoding(self):
        cases = [
            ("https://w3id.org/changes/4/aldrovandi/ttl/v2_1/ob00/1",
             "Figura Allegorica Che Rappresenta L’america"),
            ("https://w3id.org/changes/4/aldrovandi/ttl/vetrina_4_alto_s_11/ob02/1",
             "Bayle's Stephanoceras (foresti, 1887)"),
            ("https://w3id.org/changes/4/aldrovandi/ttl/vetrina_8_alto_n_3_t/ob01/1",
             "Tavoletta Con Bocca Vista Dall'Alto"),
        ]
        for iri, expected in cases:
            value = self._literal_value(iri)
            self.assertEqual(value, expected)
            self.assertTrue(("’" in value) or ("'" in value))


    # --- Time-Span: verifica tipo, lessico e datatype di begin/end ---
    def _assert_timespan(self, subj_iri: str, exp_begin: str, exp_begin_dt, exp_end: str, exp_end_dt):
        s = URIRef(subj_iri)
        # è un E52_Time-Span
        self.assertIn((s, RDF.type, self.crm["E52_Time-Span"]), self.g)

        # begin
        lit_b = self.g.value(s, self.crm.P82a_begin_of_the_begin)
        self.assertIsInstance(lit_b, Literal)
        self.assertEqual(str(lit_b), exp_begin)
        self.assertEqual(lit_b.datatype, exp_begin_dt)

        # end
        lit_e = self.g.value(s, self.crm.P82b_end_of_the_end)
        self.assertIsInstance(lit_e, Literal)
        self.assertEqual(str(lit_e), exp_end)
        self.assertEqual(lit_e.datatype, exp_end_dt)

    def test_timespan_bc_long_single_year(self):
        self._assert_timespan(
            "https://w3id.org/changes/4/aldrovandi/tsp/108/ob00/1",
            "Y-48000000-01-01", self.edtf_dt,
            "Y-48000000-12-31", self.edtf_dt
        )

    def test_timespan_dc_single_day(self):
        self._assert_timespan(
            "https://w3id.org/changes/4/aldrovandi/tsp/109/00/1",
            "2025-11-18T00:00:00Z", XSD.dateTime,
            "2025-11-18T23:59:59Z", XSD.dateTime
        )

    def test_timespan_bc_interval_shared_minus(self):
        self._assert_timespan(
            "https://w3id.org/changes/4/aldrovandi/tsp/vetrina_1_alto_s_14/ob00/1",
            "Y-10000-01-01", self.edtf_dt,
            "Y-2000-12-31", self.edtf_dt
        )

    def test_timespan_dc_interval_century(self):
        self._assert_timespan(
            "https://w3id.org/changes/4/aldrovandi/tsp/vetrina_1_alto_s_8/ob00/1",
            "0100-01-01T00:00:00Z", XSD.dateTime,
            "0199-12-31T23:59:59Z", XSD.dateTime
        )

    def test_timespan_dc_interval_regular(self):
        self._assert_timespan(
            "https://w3id.org/changes/4/aldrovandi/tsp/106/ob00/1",
            "1800-01-01T00:00:00Z", XSD.dateTime,
            "1852-12-31T23:59:59Z", XSD.dateTime
        )



if __name__ == '__main__':
    unittest.main()
