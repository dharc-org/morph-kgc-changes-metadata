__author__ = "Julián Arenas-Guerrero, Arianna Moretti"
__credits__ = ["Julián Arenas-Guerrero", "Arianna Moretti"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero, Arianna Moretti"
__email__ = ["arenas.guerrero.julian@outlook.com", "arianna.moretti4@unibo.it"]


bif_dict = {}
udf_dict = {}

# added for reading configurations
import configparser

##############################################################################
########################   BUILT-IN SCALAR FUNCTION DECORATOR   ##############
##############################################################################

# EXTRACTED FROM CONFIG FILE
# Percorso del file di configurazione
config_path = "src/morph_kgc_changes_metadata_conversions/config.ini"
# Creazione di un oggetto ConfigParser
config = configparser.ConfigParser()
# Lettura del file di configurazione
config.read(config_path)

prefisso = config["CONFIGURATION"]["project_iri_base"]
versione = config["CONFIGURATION"]["versione"]



def bif(fun_id, **params):
    """
    We borrow the idea of using decorators from pyRML by Andrea Giovanni Nuzzolese.
    """

    def wrapper(funct):
        bif_dict[fun_id] = {}
        bif_dict[fun_id]['function'] = funct
        bif_dict[fun_id]['parameters'] = params
        return funct
    return wrapper

#ADD - ARIANNA
def udf(fun_id, **params):
    """
    We borrow the idea of using decorators from pyRML by Andrea Giovanni Nuzzolese.
    """

    def wrapper(funct):
        bif_dict[fun_id] = {}
        bif_dict[fun_id]['function'] = funct
        bif_dict[fun_id]['parameters'] = params
        return funct
    return wrapper
##############################################################################
########################   GREL   ############################################
##############################################################################


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#escape',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam',
    mode='http://users.ugent.be/~bjdmeest/function/grel.ttl#modeParam')
def string_escape(string, mode):
    if mode == 'html':
        import html
        return html.escape(string)
    else:
        # TODO: not valid mode
        pass


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#string_toString',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#param_any_e')
def string_to_string(string):
    return str(string)


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#date_toDate',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam',
    format_code='http://users.ugent.be/~bjdmeest/function/grel.ttl#param_string_pattern')
def date_to_date(string, format_code):
    from datetime import datetime

    return str(datetime.strptime(string, format_code).date())


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#string_split',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam',
    separator='http://users.ugent.be/~bjdmeest/function/grel.ttl#param_string_sep')
def string_split(string, separator):
    return str(string.split(separator))


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#array_get',
    string_list='http://users.ugent.be/~bjdmeest/function/grel.ttl#param_a',
    start='http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i_from',
    end='http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i_opt_to')
def string_array_get(string_list, start, end=None):
    # it does not explode

    try:
        string_list = eval(string_list) # it is a list
    except:
        pass # it is a string

    start = int(start)
    if end:
        end = int(end)
        return str(string_list[start:end])
    else:
        return string_list[start]


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#array_slice',
    string_list='http://users.ugent.be/~bjdmeest/function/grel.ttl#param_a',
    start='http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i_from',
    end='http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i_opt_to')
def string_array_slice(string_list, start, end=None):
    # it does not explode

    try:
        string_list = eval(string_list) # it is a list
    except:
        pass # it is a string

    start = int(start)
    if end:
        end = int(end)
        return str(string_list[start:end])
    else:
        return str(string_list[start:])


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#string_replace',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam',
    old_substring='http://users.ugent.be/~bjdmeest/function/grel.ttl#param_find',
    new_substring='http://users.ugent.be/~bjdmeest/function/grel.ttl#param_replace')
def string_replace(string, old_substring, new_substring):
    return string.replace(old_substring, new_substring)


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#toLowerCase',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam')
def to_lower_case(string):
    return string.lower()


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#toUpperCase',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam')
def to_upper_case(string):
    return string.upper()


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#toTitleCase',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam')
def to_title_case(string):
    return string.title()


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#string_trim',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam')
def string_trim(string):
    return string.strip()


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#controls_if',
    boolean_expression='http://users.ugent.be/~bjdmeest/function/grel.ttl#bool_b',
    value_true='http://users.ugent.be/~bjdmeest/function/grel.ttl#any_true',
    value_false='http://users.ugent.be/~bjdmeest/function/grel.ttl#any_false')
def controls_if(boolean_expression, value_true, value_false=None):
    if eval(boolean_expression):
        return value_true
    else:
        return value_false


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#math_round',
    number='http://users.ugent.be/~bjdmeest/function/grel.ttl#param_dec_n')
def number_round(number):
    if ','  in number and '.' in number:
        number = number.replace(',', '')    # e.g. 4,894.57
    elif ',' in number:
        number = number.replace(',', '.')   # e.g. 10,7

    return str(round(float(number)))


##############################################################################
########################   OTHER   ###########################################
##############################################################################


@bif(
    fun_id='https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#controls_if_cast',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#bool_b',
    value_true='http://users.ugent.be/~bjdmeest/function/grel.ttl#any_true',
    value_false='http://users.ugent.be/~bjdmeest/function/grel.ttl#any_false')
def controls_if_cast(string, value_true, value_false=None):
    if string.lower() in ['', 'false', 'no', 'off', '0']:
        # this will be filtered when removing nulls
        return value_false
    else:
        return value_true


@bif(
    fun_id='https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#uuid')
def uuid():
    from uuid import uuid4

    return str(uuid4())


@bif(
    fun_id='https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#string_split_explode',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam',
    separator='http://users.ugent.be/~bjdmeest/function/grel.ttl#param_string_sep')
def string_split_explode(string, separator):
    return string.split(separator)


@bif(
    fun_id='https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#concat',
    string1='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam1',
    string2='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam2',
    separator='http://users.ugent.be/~bjdmeest/function/grel.ttl#param_string_sep')
def string_concat(string1, string2, separator=''):
    return f'{string1}{separator}{string2}'


@bif(
    fun_id='http://example.com/idlab/function/toUpperCaseURL',
    url='http://example.com/idlab/function/str')
def to_upper_case_url(url):
    from falcon.uri import encode_value

    url_lower = url.lower()

    if url_lower.startswith('https://'):
        return f'https://{encode_value(url[:8].upper())}'
    elif url_lower.startswith('http://'):
        return f'http://{encode_value(url[:7].upper())}'

    # else:
    return f'http://{encode_value(url.upper())}'


@bif(
    fun_id='https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#hash',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam')
def hash(string):
    from hashlib import sha256
    return sha256(string.encode("UTF-8")).hexdigest()


@bif(
    fun_id='https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#hash_iri',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam')
def hash_iri(string):
    return f'http://example.com/ns#{sha256(string.encode("UTF-8")).hexdigest()}'


### UDF - CHANGES METADATA CONVERSION

import re
import unicodedata
import datetime
from edtf import parse_edtf


@udf(
    fun_id="http://example.com/idlab/function/date_to_xsd_datetime",
    date_str="http://example.com/idlab/function/param_string_date",
    date_type="http://example.com/idlab/function/param_date_type",
)

def convert_date_to_xsd_datetime(date_str, date_type):
    """
    Converte una data nel formato 'YYYY-MM-DD'
    in un literal RDF di tipo xsd:dateTime,
    con ora fissa '00:00:00Z'.

    Esempio:
      Input:  '2023-04-17'
      Output: '"2023-04-17T00:00:00Z"^^xsd:dateTime', using datetype in the mapping
    """

    # Verifica la validità del formato,
    # solleverà ValueError se la data non è valida
    try:
        datetime.datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Formato data non valido (atteso YYYY-MM-DD): {date_str}")

    # Se la data è valida, costruisce la stringa secondo lo schema desiderato
    if date_type == "end":
        return f'{date_str}T23:59:59Z'
    else:
        return f'{date_str}T00:00:00Z'


import re


def pad_year(year_str):
    """
    Converte l'anno in intero e lo formatta a 4 cifre.
    Se l'anno è 0, lo considera come -1 (1 a.C. in notazione astronomica).
    Se l'anno (in valore assoluto) non è compreso tra 1 e 9999, restituisce None.
    """
    try:
        y = int(year_str)
    except ValueError:
        return None
    if y == 0:
        y = -1  # 1 a.C.
    if abs(y) < 1 or abs(y) > 9999:
        return None
    if y < 0:
        return '-' + str(abs(y)).zfill(4)
    else:
        return str(y).zfill(4)

import re, unicodedata

@udf(
    fun_id="http://example.com/idlab/function/split_year_range_to_dates",
    string="http://example.com/idlab/function/param_string_e",
    position="http://example.com/idlab/function/param_position_e"
)
def split_year_range_to_dates(string, position):
    if not string:
        return None

    s = unicodedata.normalize("NFKC", str(string)).strip()
    s = re.sub(r"[\u2010-\u2015\u2212]+", "-", s)         # unifica i trattini
    shared = s.lstrip().startswith("-")                   # "-" davanti?
    core = s.lstrip()[1:].strip() if shared else s
    core = re.sub(r"\s*-\s*", "-", core)
    if not core:
        return None

    # prendi 1 o 2 numeri
    if "-" in core:
        a_str, b_str = core.split("-", 1)
        try:
            a, b = int(a_str), int(b_str)
        except ValueError:
            return None
        if shared:
            a, b = -a, -b
        start, end = (a, b) if a <= b else (b, a)
    else:
        try:
            y = int(core)
        except ValueError:
            return None
        y = -y if shared else y
        start = end = y

    # formatter per ciascun estremo (indipendente)
    def fmt(y: int, m: int, d: int, is_start: bool) -> str:
        # 1..9999 → ISO xsd:dateTime; 0 o |y|>9999 o y<0 → EDTF long-year "Y±N-MM-DD"
        if 1 <= y <= 9999:
            t = "00:00:00Z" if is_start else "23:59:59Z"
            return f"{y:04d}-{m:02d}-{d:02d}T{t}"
        return f"Y{y}-{m:02d}-{d:02d}"

    pos = (position or "").strip().lower()
    if pos == "start":
        return fmt(start, 1, 1, True)
    elif pos == "end":
        return fmt(end, 12, 31, False)
    else:
        raise ValueError("Expected 'start' or 'end'")

# def convert_date_to_xsd_datetime(date_str: str) -> str:
#     """
#     Converte una data nel formato 'YYYY-MM-DD'
#     in un literal RDF di tipo xsd:dateTime,
#     con ora fissa '00:00:00Z'.
#
#     Esempio:
#       Input:  '2023-04-17'
#       Output: '"2023-04-17T00:00:00Z"^^xsd:dateTime'
#     """
#
#     # Verifica la validità del formato,
#     # solleverà ValueError se la data non è valida
#     try:
#         datetime.datetime.strptime(date_str, "%Y-%m-%d")
#     except ValueError:
#         raise ValueError(f"Formato data non valido (atteso YYYY-MM-DD): {date_str}")
#
#     # Se la data è valida, costruisce la stringa secondo lo schema desiderato
#     return f"\"{date_str}T00:00:00Z\"^^xsd:dateTime"


@udf(
    fun_id="http://example.com/idlab/function/assess_aat_tool_type",
    tecnica="http://example.com/idlab/function/acquisition_technique"
)
def retrieve_tool_type(tecnica):
    # remove spaces at beginning and end + adjust internal spaces
    if convert_to_aat(tecnica) == "aat:300391312":
        return "aat:300429747"
    elif convert_to_aat(tecnica) == "aat:300053580":
        return "aat:300266792"


@udf(
    fun_id="http://example.com/idlab/function/convert_to_aat",
    tecnica="http://example.com/idlab/function/param_tecnica"
)
def convert_to_aat(tecnica):
    # remove spaces at beginning and end + adjust internal spaces
    tecnica = " ".join(tecnica.strip().split())

    # uppercase fist char
    tecnica = tecnica.capitalize()

    # Mapping techniques
    mapping_tecniche = {
        "Acquerello": "aat:300053363",
        "Affresco": "aat:300053357",
        "Disegno": "aat:300054196",
        "Essiccatura": "aat:300053758",
        "Incisione": "aat:300053225",
        "Intarsio": "aat:300053852",
        "Modellazione cera": "aat:300265248",
        "Mosaico": "aat:300138684",
        "Olio su tela": "aat:300178684",
        "Pittura su vaso": "aat:300343820",
        "Scrittura": "aat:300252927",
        "Scultura": "aat:300264383",
        "Stampa": "aat:300053319",
        "Stampa tipografica": "aat:300178926",
        "Tassidermia": "aat:300053628",
        "Fotogrammetria": "aat:300053580",
        "Scanner artec": "aat:300391312",
        "Scansione a proiezione di luce strutturata": "aat:300391312",
    }

    # return mapped aat value, none if not present
    return mapping_tecniche.get(tecnica, None)


@udf(
    fun_id="http://example.com/idlab/function/normalize_author_name",
    author_name="http://example.com/idlab/function/param_author_name"
)

def normalize_author_name(author_name):
    # strip whitespace
    author_name = author_name.strip()

    # regex to extract the name, ignoring everything inside parentheses
    name_part = re.sub(r"\(.*?\)", "", author_name).strip()

    # to lowercase
    normalized_name = name_part.lower()

    # remove accents
    normalized_name = unicodedata.normalize('NFKD', normalized_name).encode('ascii', 'ignore').decode('ascii')

    # replace spaces and non-alphanumeric characters with hyphens
    normalized_name = re.sub(r'[^a-z0-9]+', '-', normalized_name)

    # remove trailing hyphens
    normalized_name = normalized_name.strip('-')

    return f"ex:{normalized_name}"  # Return the normalized name


@udf(
    fun_id="http://example.com/idlab/function/extract_documented_in_iri",
    param_author_name="http://example.com/idlab/function/p_author_name"
)
def extract_documented_in_iri(param_author_name=None):
    """
    Extracts the VIAF, ORCID or ULAN ID from the author's string and constructs the corresponding IRI.
    :param param_author_name: The string containing the author's name and the VIAF or ULAN ID.
    :return: The IRI corresponding to the VIAF or ULAN ID.
    """
    if param_author_name is None:
        return None
    param_author_name = param_author_name.lower()
    # Pattern VIAF
    viaf_pattern = r'\(viaf:(\d+)\)'
    # Pattern ULAN
    ulan_pattern = r'\(ulan:(\d+)\)'

    # Pattern ORCID
    orcid_pattern = r'\(orcid:(\d+)\)'

    # extracts ID VIAF
    viaf_match = re.search(viaf_pattern, param_author_name)
    if viaf_match:
        viaf_id = viaf_match.group(1)
        return f"http://viaf.org/viaf/{viaf_id}"

    # extracts ID ULAN
    ulan_match = re.search(ulan_pattern, param_author_name)
    if ulan_match:
        ulan_id = ulan_match.group(1)
        return f"http://vocab.getty.edu/ulan/{ulan_id}"

    # extracts ID ORCID
    orcid_match = re.search(orcid_pattern, param_author_name)
    if orcid_match:
        orcid_id = orcid_match.group(1)
        return f"https://orcid.org/{orcid_id}"


    # return None if no ID was found
    return None



@udf(
    fun_id="http://example.com/idlab/function/extract_title",
    param_title_original="http://example.com/idlab/function/p_title_original"
)
def extract_title(param_title_original=None):
    """
    Extracts the title from the string in the format "Title @lang".
    :param param_title_original: The string containing the title and language in the format "Title @lang".
    :return: The title without the language code.
    """
    if param_title_original is None:
        return None

    # title pattern
    pattern = r'^(.*) @(\w+)$'
    match = re.match(pattern, param_title_original)

    if match:
        title = match.group(1).strip()
        return title

    return param_title_original


@udf(
    fun_id="http://example.com/idlab/function/convert_documentary_type_to_aat",
    documentary_type="http://example.com/idlab/function/param_documentary_type"
)
def convert_documentary_type_to_aat(documentary_type):
    """
    Mappa la tipologia documentaria al suo corrispondente codice Getty AAT.
    Non ritorna nulla se il campo è vuoto o non è una stringa.

    :param documentary_type: La tipologia documentaria.
    :return: Il codice AAT corrispondente, o None se non trovato o se vuoto.
    """
    # check if the value is a string
    if not isinstance(documentary_type, str):
        return None

    # remove spaces at beginning and end + adjust internal spaces
    documentary_type = " ".join(documentary_type.strip().split())

    # None if the field is empty
    if not documentary_type:
        return None

    # mapping for primary source and parent source documentary type
    mapping_tipologia = {
        '': None,
        'Bottiglia': 'aat:300045627',
        'Volume a stampa': 'aat:300265632',
        'Volume manoscritto': 'aat:300403970',
        'Vaso': 'aat:300132254',
        'Stampa': 'aat:300041273',
        'Xilografia': 'aat:300041405',
        'Maschera': 'aat:300138758',
        "Opera d'arte": 'aat:300191086',
        'Sonaglio': 'aat:300041933',
        'Strumento tecnico': 'aat:300122241',
        'Carta nautica': 'aat:300028309',
        'Ascia': 'aat:300420536',
        'Arco Scaricatore': 'aat:300433476',
        'Artefatto': 'aat:300117127',
        'Medaglia': 'aat:300046025',
        'Manico di coltello': 'aat:300024928',
        'Erbario': 'aat:300440768',
        'Diorama': 'aat:300047762',
        'Tavola manoscritta': 'aat:300028569',
        'Collana': 'aat:300046001',
        'Manoscritto miniato': 'aat:300265483',
        'Dipinto': 'aat:300033618',
        'Codice manoscritto': 'aat:300224200',
        'Statua': 'aat:300047600',
        'Gemma': 'aat:300011172',
        'Compasso': 'aat:300022488',
        'Mappa': 'aat:300028094',
        'Video': 'aat:300028682',
        'Macchina elettrostatica': 'aat:300425064',
        'Calco': 'aat:300024814',
        'Modello': 'aat:300047753',
        'Lampada': 'aat:300037592',
        'Microscopio': 'aat:300024594',
        'Specimen': 'aat:300235576',
        'Pendente': 'aat:300422994',
        'Serie di stampe': 'aat:300189634',
        'Serie di volumi a stampa': 'aat:300027349',
        'Serie di dipinti': 'aat:300226836',
        'Incunabolo': 'aat:300055021',
        'Serie di affreschi': 'aat:300184300',
        'Atlante nautico': 'aat:300137376'
    }

    # return AAT correspondig code, None if no mapping is found
    return mapping_tipologia.get(documentary_type, None)


@udf(
    fun_id="http://example.com/idlab/function/normalize_id_number",
    param_name="http://example.com/idlab/function/id_number",
)
def normalize_id_number(param_name, suffix):
    # strip whitespace
    param_name = param_name.replace(" ", "")

    # to upper
    normalized_name = param_name.upper()

    # remove accents
    normalized_name = unicodedata.normalize('NFKD', normalized_name).encode('ascii', 'ignore').decode('ascii')

    # return the URI with the suffix
    return normalized_name


## DA ELIMINARE E SOSTITUIRE CON NORMALIZE IRI E ADD/REMOVE STRING

@udf(
    fun_id="http://example.com/idlab/function/normalize_and_suffix",
    param_name="http://example.com/idlab/function/param_name",
    suffix="http://example.com/idlab/function/param_suffix"
)
def normalize_and_suffix(param_name, suffix):
    # strip whitespace
    param_name = param_name.strip()

    # regex to extract the main name part, ignoring everything inside parentheses
    name_part = re.sub(r"\(.*?\)", "", param_name).strip()

    # to lowercase
    normalized_name = name_part.lower()

    # remove accents
    normalized_name = unicodedata.normalize('NFKD', normalized_name).encode('ascii', 'ignore').decode('ascii')

    # replacement of spaces and non-alphanumeric characters with hyphens
    normalized_name = re.sub(r'[^a-z0-9]+', '-', normalized_name)

    # removing hyphens
    normalized_name = normalized_name.strip('-')

    # return the URI with the suffix
    return f"ex:{normalized_name}{suffix}"


@udf(
    fun_id='http://example.com/idlab/function/normalize_str_param',
    str_param='http://example.com/idlab/function/valParam'
)
def normalize_str_param(str_param):
    str_param = re.sub(r"[\(\[].*?[\)\]]", "", str_param)
    str_param = str_param.strip().lower()
    str_param = unicodedata.normalize('NFKD', str_param).encode('ascii', 'ignore').decode('ascii')
    str_param = re.sub(r"\s+", " ", str_param)
    str_param = str_param.replace(' ', '_')
    str_param = str_param.replace('"', '')
    str_param = str_param.replace('.', '_')
    str_param = str_param.strip('_')
    str_param = re.sub(r"_+", "_", str_param)
    return str_param


@udf(
    fun_id='http://example.com/idlab/function/normalize_and_convert_to_iri',
    str_param='http://example.com/idlab/function/valueParams',
    type_param='http://example.com/idlab/function/valueType',
    num_param='http://example.com/idlab/function/valueNum',
    parent_param='http://example.com/idlab/function/valueParent'
)
def normalize_and_convert_to_iri(str_param, type_param, num_param, parent_param=None):
    # Pulizia stringa
    str_param = normalize_str_param(str_param)

    # Normalizzazione num_param → due cifre se numerico
    if num_param != "":
        try:
            num_param = f"{int(num_param):02d}"
        except ValueError:
            pass

    if not parent_param:
        if num_param == "":
            return "".join([prefisso, type_param, "/", str_param, "/", versione])
        else:
            return "".join([prefisso, type_param, "/", str_param, "/", num_param, "/", versione])
    else:
        if num_param == "":
            return "".join([prefisso, type_param, "/", str_param, "_parent", "/", versione])
        else:
            return "".join([prefisso, type_param, "/", str_param, "_parent", "/", num_param, "/", versione])



@udf(
    fun_id="http://example.com/idlab/function/conditional_normalize_and_convert_to_iri",
    str_param='http://example.com/idlab/function/valueParams',
    type_param='http://example.com/idlab/function/valueType',
    num_param='http://example.com/idlab/function/valueNum',
    relazione="http://example.com/idlab/function/relazione",
    relazione_target="http://example.com/idlab/function/relazione_target",
    parent_param='http://example.com/idlab/function/valueParent'
)
def conditional_normalize_and_suffix(str_param, type_param, num_param, relazione, relazione_target, parent_param=None):
    # Se la relazione non è uguale a relazione_target, restituisce None
    if relazione.strip().lower() != relazione_target.strip().lower():
        return None
    # Altrimenti normalizza numero_collegato e suffisso, e li unisce
    norm_iri = normalize_and_convert_to_iri(str_param, type_param, num_param)

    return norm_iri

@udf(
    fun_id="http://example.com/idlab/function/multiple_separator_split_explode",
    string='http://example.com/idlab/function/valParam',
    separators_list_str='http://example.com/idlab/function/list_param_string_sep'
    )

def multi_sep_string_split_explode(string, separators_list_str):
    separators_list = separators_list_str.split("---")
    for s in separators_list:
        string = string.replace(s, "---")
    string_list = string.split("---")
    clean_string_list_exploded = [x.strip() for x in string_list if x]
    return([el for el in clean_string_list_exploded if el])


@udf(
    fun_id="http://example.com/idlab/function/sequential_iris",
    iris_n='http://example.com/idlab/function/number_of_iris')

def generate_sequential_iris(iris_n):
    iris_n =int(iris_n)
    iris_list_result = []
    for n in range(iris_n):
        if iris_n < 10:
            n_string = "0" + str(n)
        else:
            n_string = str(n)
        iris_list_result.append(n_string)
    return iris_list_result

@udf(
    fun_id="http://example.com/idlab/function/digital_model_label",
    idx="http://example.com/idlab/function/idx",
    nr="http://example.com/idlab/function/nr",
)
def digital_model_label(idx, nr):
    """
    Ritorna 'Modello Digitale <idx> oggetto <nr>'.
    """
    s_idx = str(idx).strip()
    if s_idx.isdigit():
        s_idx = f"{int(s_idx):02d}"
    else:
        s_idx = s_idx.zfill(2)
    s_nr = str(nr).strip()
    return f"Modello Digitale {s_idx} oggetto {s_nr}"

@udf(
    fun_id="http://example.com/idlab/function/extract_and_derivate_sub_str",
    string='http://example.com/idlab/function/valParamStr_inputBase',
    string_nd='http://example.com/idlab/function/valParamStr_secondInput',
    mode='http://example.com/idlab/function/valParamStr_deriveOrExtract',
    use_prefix = 'http://example.com/idlab/function/use_prefix_opt',
    suffix = 'http://example.com/idlab/function/sfx')

def extract_and_derivate_strings(string, string_nd, mode, use_prefix=False, suffix=None):
    global prefisso

    if not use_prefix:
        local_prefix = ''
    else:
       local_prefix = prefisso

    if not suffix:
        sfx = ''
    else:
        sfx = suffix

    mode = mode.lower().strip()
    output_iri = ""
    if mode =="add":
        output_iri = "-".join([string,string_nd])
    elif mode == "remove":
        output_iri = re.sub(string_nd, "", string)
    iri_core = normalize_and_convert_to_iri(output_iri)
    if type(iri_core) is list:
        iri_core = str(iri_core).replace("[","").replace("]", "")
    return str(local_prefix) + iri_core + str(sfx)



# https://w3id.org/changes/4/aldrovandi/%3Cnr%3E/---/00



