""" Morph-KGC """

__author__ = "Julián Arenas-Guerrero"
__copyright__ = "Copyright (C) 2020 Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import rdflib
import logging
import os
import sql_metadata
import rfc3987
import pandas as pd
import constants
import utils

from mapping.mapping_partitioner import MappingPartitioner
from data_source import relational_source


def _mapping_to_rml(mapping_graph, config_section_name):
    """
    Recognizes the mapping language of the rules in a graph. If it is R2RML, the mapping rules are converted to RML.
    """

    r2rml_query = 'SELECT ?s WHERE {?s <http://www.w3.org/ns/r2rml#logicalTable> ?o .} LIMIT 1 '
    # if the query result set is not empty then the mapping language is R2RML
    if len(mapping_graph.query(r2rml_query)) > 0:
        logging.debug("Data source `" + config_section_name + "` has R2RML rules, converting them to RML.")

        # replace R2RML predicates with the equivalent RML predicates
        mapping_graph = utils.replace_predicates_in_graph(mapping_graph, constants.R2RML['logical_table'],
                                                          constants.RML['logical_source'])
        mapping_graph = utils.replace_predicates_in_graph(mapping_graph, constants.R2RML['sql_query'],
                                                          constants.RML['query'])
        mapping_graph = utils.replace_predicates_in_graph(mapping_graph, constants.R2RML['column'],
                                                          constants.RML['reference'])

    return mapping_graph


def _get_join_object_maps_join_conditions(join_query_results):
    """
    Creates a dictionary with the results of the JOIN_CONDITION_PARSING_QUERY. The keys are the identifiers of the
    child triples maps of the join condition. The values of the dictionary are in turn other dictionaries with two
    items with keys, child_value and parent_value, representing a join condition.
    """

    join_conditions_dict = {}

    for join_condition in join_query_results:
        # add the child triples map identifier if it is not in the dictionary
        if join_condition.object_map not in join_conditions_dict:
            join_conditions_dict[join_condition.object_map] = {}

        # add the new join condition (note that several join conditions can apply in a join)
        join_conditions_dict[join_condition.object_map][str(join_condition.join_condition)] = \
            {'child_value': str(join_condition.child_value), 'parent_value': str(join_condition.parent_value)}

    return join_conditions_dict


def _append_mapping_rule(data_source_mappings_df, mapping_rule):
    """
    Builds a Pandas DataFrame from the results obtained from MAPPING_PARSING_QUERY and
    JOIN_CONDITION_PARSING_QUERY for one source.
    """

    # get position for the new mapping rule in the DataFrame
    i = len(data_source_mappings_df)

    data_source_mappings_df.at[i, 'triples_map_id'] = mapping_rule.triples_map_id
    data_source_mappings_df.at[i, 'data_source'] = mapping_rule.data_source
    data_source_mappings_df.at[i, 'object_map'] = mapping_rule.object_map
    data_source_mappings_df.at[i, 'ref_form'] = mapping_rule.ref_form
    data_source_mappings_df.at[i, 'iterator'] = mapping_rule.iterator
    data_source_mappings_df.at[i, 'tablename'] = mapping_rule.tablename
    data_source_mappings_df.at[i, 'query'] = mapping_rule.query
    data_source_mappings_df.at[i, 'subject_template'] = mapping_rule.subject_template
    data_source_mappings_df.at[i, 'subject_reference'] = mapping_rule.subject_reference
    data_source_mappings_df.at[i, 'subject_constant'] = mapping_rule.subject_constant
    data_source_mappings_df.at[i, 'subject_rdf_class'] = mapping_rule.subject_rdf_class
    data_source_mappings_df.at[i, 'subject_termtype'] = mapping_rule.subject_termtype
    data_source_mappings_df.at[i, 'graph_constant'] = mapping_rule.graph_constant
    data_source_mappings_df.at[i, 'graph_template'] = mapping_rule.graph_template
    data_source_mappings_df.at[i, 'graph_reference'] = mapping_rule.graph_reference
    data_source_mappings_df.at[i, 'predicate_constant'] = mapping_rule.predicate_constant
    data_source_mappings_df.at[i, 'predicate_template'] = mapping_rule.predicate_template
    data_source_mappings_df.at[i, 'predicate_reference'] = mapping_rule.predicate_reference
    data_source_mappings_df.at[i, 'object_constant'] = mapping_rule.object_constant
    data_source_mappings_df.at[i, 'object_template'] = mapping_rule.object_template
    data_source_mappings_df.at[i, 'object_reference'] = mapping_rule.object_reference
    data_source_mappings_df.at[i, 'object_termtype'] = mapping_rule.object_termtype
    data_source_mappings_df.at[i, 'object_datatype'] = mapping_rule.object_datatype
    data_source_mappings_df.at[i, 'object_language'] = mapping_rule.object_language
    data_source_mappings_df.at[i, 'object_parent_triples_map'] = mapping_rule.object_parent_triples_map
    data_source_mappings_df.at[i, 'predicate_object_graph_constant'] = mapping_rule.predicate_object_graph_constant
    data_source_mappings_df.at[
        i, 'predicate_object_graph_reference'] = mapping_rule.predicate_object_graph_reference
    data_source_mappings_df.at[i, 'predicate_object_graph_template'] = mapping_rule.predicate_object_graph_template

    return data_source_mappings_df


def _validate_no_repeated_triples_maps(mapping_graph, source_name):
    """
    Checks that there are no repeated triples maps in the mapping rules of a source. This is important because
    if there are repeated triples maps (i.e. triples map with the same identifier), it is not possible to process
    parent triples maps correctly.
    """

    query = """
        prefix rr: <http://www.w3.org/ns/r2rml#>
        SELECT ?triples_map_id WHERE { ?triples_map_id rr:subjectMap ?_subject_map . } """

    # get the identifiers of all the triples maps in the graph
    triples_map_ids = [str(result.triples_map_id) for result in list(mapping_graph.query(query))]

    # get the identifiers that are repeated
    repeated_triples_map_ids = utils.get_repeated_elements_in_list(triples_map_ids)

    # if there are any repeated identifiers, then it will produce errors during materialization
    if len(repeated_triples_map_ids) > 0:
        raise Exception("The following triples maps in data source `" + source_name + "` are repeated: " +
                        str(repeated_triples_map_ids) + '.')


def _transform_mappings_into_dataframe(mapping_query_results, join_query_results, config_section_name):
    """
    Builds a Pandas DataFrame from the results obtained from MAPPING_PARSING_QUERY and
    JOIN_CONDITION_PARSING_QUERY for one source.
    """

    # create empty DataFrame with relevant columns
    source_mappings_df = pd.DataFrame(columns=constants.MAPPINGS_DATAFRAME_COLUMNS)
    # populate the DataFrame with parsed mappings from parsing SPARQL query
    for mapping_rule in mapping_query_results:
        source_mappings_df = _append_mapping_rule(source_mappings_df, mapping_rule)

    # process mapping rules with joins
    # create dict with child triples maps in the keys and its join conditions in in the values
    join_conditions_dict = _get_join_object_maps_join_conditions(join_query_results)
    # map the dict with the join conditions to the mapping rules in the DataFrame
    source_mappings_df['join_conditions'] = source_mappings_df['object_map'].map(join_conditions_dict)
    # needed for later hashing the dataframe
    source_mappings_df['join_conditions'] = source_mappings_df['join_conditions'].where(
        pd.notnull(source_mappings_df['join_conditions']), '')
    # convert the join condition dictionaries to string (can later be converted back to dict)
    source_mappings_df['join_conditions'] = source_mappings_df['join_conditions'].astype(str)
    # object_map column no longer needed, remove it
    source_mappings_df = source_mappings_df.drop('object_map', axis=1)

    # link the mapping rules to its data source name
    source_mappings_df['source_name'] = config_section_name

    return source_mappings_df


def _is_delimited_identifier(identifier):
    """
    Checks if an identifier is delimited or not.
    """

    if len(identifier) > 2:
        if identifier[0] == '"' and identifier[len(identifier) - 1] == '"':
            return True
    return False


def _get_undelimited_identifier(identifier):
    """
    Removes delimiters from the identifier if it is delimited.
    """

    if pd.notna(identifier):
        identifier = str(identifier)
        if _is_delimited_identifier(identifier):
            return identifier[1:-1]
    return identifier


def _get_valid_template_identifiers(template):
    """
    Removes delimiters from delimited identifiers in a template.
    """

    if pd.notna(template):
        return template.replace('{"', '{').replace('"}', '}')
    return template


def _validate_parsed_mappings(mappings_df):
    """
    Checks that the mapping rules in the input DataFrame are valid. If something is wrong in the mappings the
    execution is stopped. Specifically it is checked that termtypes are correct, constants and templates are valid
    IRIs and that language tags and datatypes are used properly. Also checks that different data sources do not
    have triples map with the same id.
    """
    # TODO: I would do validation outside the mapping parser (maybe when validating mappings against ontology)

    # check termtypes are correct (i.e. that they are rr:IRI, rr:BlankNode or rr:Literal and that subject map is
    # not a rr:literal). Use subset operation
    if not (set(mappings_df['subject_termtype'].astype(str)) <= {constants.R2RML['IRI'],
                                                                 constants.R2RML['blank_node']}):
        raise ValueError('Found an invalid subject termtype. Found values ' + str(set(
            mappings_df['subject_termtype'].astype(str))) + '. Subject maps must be rr:IRI or rr:BlankNode.')
    if not (set(mappings_df['object_termtype'].astype(str)) <= {constants.R2RML['IRI'],
                                                                constants.R2RML['blank_node'],
                                                                constants.R2RML['literal']}):
        raise ValueError('Found an invalid object termtype. Found values ' + str(set(
            mappings_df['subject_termtype'].astype(
                str))) + '. Object maps must be rr:IRI, rr:BlankNode or rr:Literal.')

    # if there is a datatype or language tag then the object map termtype must be a rr:Literal
    if len(mappings_df.loc[
               (mappings_df['object_termtype'] != constants.R2RML['literal']) & mappings_df[
                'object_datatype'].notnull() & mappings_df['object_language'].notnull()]) > 0:
        raise Exception('Found object maps with a language tag or a datatype, '
                        'but that do not have termtype rr:Literal.')

    # language tags and datatypes cannot be used simultaneously, language tags are used if both are given
    if len(mappings_df.loc[
               mappings_df['object_language'].notnull() & mappings_df['object_datatype'].notnull()]) > 0:
        logging.warning('Found object maps with a language tag and a datatype. Both of them cannot be used '
                        'simultaneously for the same object map, and the language tag has preference.')

    # check constants are valid IRIs. Get all constants in predicate, graph, subject and object
    constants_terms = list(mappings_df['predicate_constant'].dropna())
    constants_terms.extend(list(mappings_df['graph_constant'].dropna()))
    constants_terms.extend(list(mappings_df.loc[
                                    (mappings_df['subject_termtype'] == constants.R2RML['IRI']) &
                                    mappings_df['subject_constant'].notnull()]['subject_constant']))
    constants_terms.extend(list(mappings_df.loc[
                                    (mappings_df['object_termtype'] == constants.R2RML['IRI']) &
                                    mappings_df['object_constant'].notnull()]['object_constant']))
    # validate that each of the constants retrieved are valid URIs
    for constant in set(constants_terms):
        rfc3987.parse(constant, rule='IRI')

    # check templates are valid IRIs. Get all templates in predicate, graph, subject and object
    templates = list(mappings_df['predicate_template'].dropna())
    templates.extend(list(mappings_df['graph_template'].dropna()))
    templates.extend(list(mappings_df.loc[
                              (mappings_df['subject_termtype'] == constants.R2RML['IRI']) & mappings_df[
                                  'subject_template'].notnull()]['subject_template']))
    templates.extend(list(mappings_df.loc[
                              (mappings_df['object_termtype'] == constants.R2RML['IRI']) & mappings_df[
                                  'object_template'].notnull()]['object_template']))
    for template in templates:
        # validate that at least the INVARIABLE part of the template is a valid IRI
        rfc3987.parse(utils.get_invariable_part_of_template(str(template)), rule='IRI')

    # check that a triples map id is not repeated in different data sources
    # Get unique source names and triples map identifiers
    aux_mappings_df = mappings_df[['source_name', 'triples_map_id']].drop_duplicates()
    # get repeated triples map identifiers
    repeated_triples_map_ids = utils.get_repeated_elements_in_list(
        list(aux_mappings_df['triples_map_id'].astype(str)))
    # of those repeated identifiers, ignore those that come from rr:class (i.e. have the prefix rdf_class_)
    repeated_triples_map_ids = [tm_id for tm_id in repeated_triples_map_ids if not tm_id.startswith('rdf_class_')]
    if len(repeated_triples_map_ids) > 0:
        raise Exception('The following triples maps appear in more than one data source: ' +
                        str(repeated_triples_map_ids) +
                        '. Check the mapping files, one triple map cannot be repeated in different data sources.')


class MappingParser:

    def __init__(self, config):
        self.mappings_df = pd.DataFrame(columns=constants.MAPPINGS_DATAFRAME_COLUMNS)
        self.config = config

    def __str__(self):
        return str(self.mappings_df)

    def __repr__(self):
        return repr(self.mappings_df)

    def __len__(self):
        return len(self.mappings_df)

    def parse_mappings(self):
        """
        Parses the mapping files in each source of the input config file. It also checks that the parsed mappings are
        valid.
        """

        # parse mapping files for each data source in the config file and add the parsed mappings rules to a
        # common DataFrame for all data sources
        for config_section_name in self.config.sections():
            if config_section_name != 'CONFIGURATION':
                data_source_mappings_df = self._parse_data_source_mapping_files(config_section_name)
                self.mappings_df = pd.concat([self.mappings_df, data_source_mappings_df])
                self.mappings_df = self.mappings_df.reset_index(drop=True)

        self._normalize_mappings()
        self._infer_datatypes()
        _validate_parsed_mappings(self.mappings_df)

        logging.info(str(len(self.mappings_df)) + ' mapping rules retrieved.')

        # generate mapping partitions
        mapping_partitioner = MappingPartitioner(self.mappings_df, self.config)
        self.mappings_df = mapping_partitioner.partition_mappings()

        return self.mappings_df

    def _parse_data_source_mapping_files(self, config_section_name):
        """
        Creates a Pandas DataFrame with the mapping rules for a data source. It loads the mapping files in a rdflib
        graph and recognizes the mapping language used. Mapping files serialization is automatically guessed.
        It performs queries MAPPING_PARSING_QUERY and JOIN_CONDITION_PARSING_QUERY and process the results to build a
        DataFrame with the mapping rules. Also verifies that there are not repeated triples maps in the mappings.
        """

        mapping_graph = rdflib.Graph()
        try:
            # process all file/directories given for as mappings for the data source
            for mapping_path in self.config.get(config_section_name, 'mappings').split(','):
                # if it is a file load the mapping triples to the graph
                if os.path.isfile(mapping_path):
                    mapping_graph.load(mapping_path, format=rdflib.util.guess_format(mapping_path))
                # if it is a directory process all the mapping files within the root of the directory
                elif os.path.isdir(mapping_path):
                    for mapping_file_name in os.listdir(mapping_path):
                        mapping_file = os.path.join(mapping_path, mapping_file_name)
                        if os.path.isfile(mapping_file):
                            mapping_graph.load(mapping_file, format=rdflib.util.guess_format(mapping_file))
        except Exception as n3_mapping_parse_exception:
            raise Exception(n3_mapping_parse_exception)

        # before further processing, convert R2RML rules to RML, so that we can assume RML for parsing
        mapping_graph = _mapping_to_rml(mapping_graph, config_section_name)

        # parse the mappings with the parsing query
        mapping_query_results = mapping_graph.query(constants.MAPPING_PARSING_QUERY)
        join_query_results = mapping_graph.query(constants.JOIN_CONDITION_PARSING_QUERY)

        # check triples maps are not repeated, which would lead to errors (because of repeated triples maps identifiers)
        _validate_no_repeated_triples_maps(mapping_graph, config_section_name)

        # convert the SPARQL result set with the parsed mappings to DataFrame
        return _transform_mappings_into_dataframe(mapping_query_results, join_query_results, config_section_name)

    def _normalize_mappings(self):
        # start by removing duplicated triples
        self.mappings_df = self.mappings_df.drop_duplicates()
        # convert rr:class to new POMs
        self._rdf_class_to_pom()
        # normalizes graphs terms in the mappings
        self._process_pom_graphs()
        # if a term as no associated rr:termType, complete it as indicated in R2RML specification
        self._complete_termtypes()

        # we want to track the type of data source (RDB, CSV, EXCEL, JSON, etc) in the parsed mapping rules
        for i, mapping_rule in self.mappings_df.iterrows():
            self.mappings_df.at[i, 'source_type'] = self.config.get(mapping_rule['source_name'], 'source_type')

        # ignore the delimited identifiers (this is not conformant with R2MRL specification)
        self._remove_delimiters_from_mappings()

        # create a unique id for each mapping rule
        self.mappings_df.insert(0, 'id', self.mappings_df.reset_index(drop=True).index)

    def _rdf_class_to_pom(self):
        """
        Transforms rr:class properties (subject_rdf_class column in the input DataFrame) into POMs. The new mapping
        rules corresponding to rr:class properties are added to the input DataFrame and subject_rdf_class column is
        removed.
        """

        # make a copy of the parsed mappings
        initial_mapping_df = self.mappings_df.copy()

        # iterate over the mapping rules
        for i, row in initial_mapping_df.iterrows():
            # if a mapping rules has rr:class, generate a new POM to generate triples for this graph
            if pd.notna(row['subject_rdf_class']):
                # get the position of the new POM in the DataFrame
                j = len(self.mappings_df)

                # build the new POM from the mapping rule
                self.mappings_df.at[j, 'source_name'] = row['source_name']
                # add rdf_class_ at the beginning to avoid duplciating triples map identifiers
                self.mappings_df.at[j, 'triples_map_id'] = 'rdf_class_' + str(row['triples_map_id'])
                self.mappings_df.at[j, 'tablename'] = row['tablename']
                self.mappings_df.at[j, 'query'] = row['query']
                self.mappings_df.at[j, 'subject_template'] = row['subject_template']
                self.mappings_df.at[j, 'subject_reference'] = row['subject_reference']
                self.mappings_df.at[j, 'subject_constant'] = row['subject_constant']
                self.mappings_df.at[j, 'graph_constant'] = row['graph_constant']
                self.mappings_df.at[j, 'graph_reference'] = row['graph_reference']
                self.mappings_df.at[j, 'graph_template'] = row['graph_template']
                self.mappings_df.at[j, 'subject_termtype'] = row['subject_termtype']
                self.mappings_df.at[j, 'predicate_constant'] = constants.RDF['type']
                self.mappings_df.at[j, 'object_constant'] = row['subject_rdf_class']
                self.mappings_df.at[j, 'object_termtype'] = constants.R2RML['IRI']
                self.mappings_df.at[j, 'join_conditions'] = ''

        # subject_rdf_class column no longer needed, remove it
        self.mappings_df = self.mappings_df.drop('subject_rdf_class', axis=1)
        # ensure that we do not generate duplicated mapping rules
        self.mappings_df = self.mappings_df.drop_duplicates()

    def _process_pom_graphs(self):
        """
        Completes mapping rules in the input DataFrame with rr:defaultGraph if any graph term is provided for that
        mapping rule (as indicated in R2RML specification
        (https://www.w3.org/2001/sw/rdb2rdf/r2rml/#generated-triples)). Also simplifies the DataFrame unifying graph
        terms in one column (graph_constant, graph_template, graph_reference).
        """

        # use rr:defaultGraph for those mapping rules that do not have any graph term
        for i, mapping_rule in self.mappings_df.iterrows():
            # check the POM has no associated graph term
            if pd.isna(mapping_rule['graph_constant']) and pd.isna(mapping_rule['graph_reference']) and \
                    pd.isna(mapping_rule['graph_template']):
                if pd.isna(mapping_rule['predicate_object_graph_constant']) and \
                        pd.isna(mapping_rule['predicate_object_graph_reference']) and \
                        pd.isna(mapping_rule['predicate_object_graph_template']):
                    # no graph term for this POM, assign rr:defaultGraph
                    self.mappings_df.at[i, 'graph_constant'] = constants.R2RML['default_graph']

        # instead of having two columns for graph terms (one for subject maps, i.e. graph_constant, and other for POMs,
        # i.e. predicate_object_graph_constant), keep only one for simplicity. In order
        # to have only one column, append POM graph terms as new mapping rules in the DataFrame.
        for i, mapping_rule in self.mappings_df.copy().iterrows():
            if pd.notna(mapping_rule['predicate_object_graph_constant']):
                # position of the new rule in the DataFrame
                j = len(self.mappings_df)
                # copy (duplicate) the mapping rule
                self.mappings_df.loc[j] = mapping_rule
                # update the graph term (with the POM graph term) of the new mapping rule
                self.mappings_df.at[j, 'graph_constant'] = mapping_rule['predicate_object_graph_constant']
            if pd.notna(mapping_rule['predicate_object_graph_template']):
                j = len(self.mappings_df)
                self.mappings_df.loc[j] = mapping_rule
                self.mappings_df.at[j, 'graph_template'] = mapping_rule['predicate_object_graph_template']
            if pd.notna(mapping_rule['predicate_object_graph_reference']):
                j = len(self.mappings_df)
                self.mappings_df.loc[j] = mapping_rule
                self.mappings_df.at[j, 'graph_reference'] = mapping_rule['predicate_object_graph_reference']

        # remove POM graph columns
        self.mappings_df = self.mappings_df.drop(
            columns=['predicate_object_graph_constant', 'predicate_object_graph_template',
                     'predicate_object_graph_reference'])
        # Drop where graph_constant, graph_template and graph_reference are null. This is because it can happen that
        # original mapping rules have graph term for subject maps but not for POM, and if this happens, the newly
        # appended mapping rule applies, but the the old one (without graph term in the subject map) must not be
        # considered.
        self.mappings_df = self.mappings_df.dropna(subset=['graph_constant', 'graph_template', 'graph_reference'],
                                                   how='all')
        # ensure that we do not introduce duplicate mapping rules
        self.mappings_df = self.mappings_df.drop_duplicates()

    def _complete_termtypes(self):
        """
        Completes term types of mapping rules that do not have rr:termType property as indicated in R2RML specification
        (https://www.w3.org/2001/sw/rdb2rdf/r2rml/#termtype).
        """

        for i, mapping_rule in self.mappings_df.iterrows():
            # if subject termtype is missing, then subject termtype is rr:IRI
            if pd.isna(mapping_rule['subject_termtype']):
                self.mappings_df.at[i, 'subject_termtype'] = constants.R2RML['IRI']

            if pd.isna(mapping_rule['object_termtype']):
                # if object termtype is missing and there is a language tag or datatype or object term is a
                # reference, then termtype is rr:Literal
                if pd.notna(mapping_rule['object_language']) or pd.notna(mapping_rule['object_datatype']) or \
                        pd.notna(mapping_rule['object_reference']):
                    self.mappings_df.at[i, 'object_termtype'] = constants.R2RML['literal']

                else:
                    # if previous conditions (language tag, datatype or reference) do not hold, then termtype is rr:IRI
                    self.mappings_df.at[i, 'object_termtype'] = constants.R2RML['IRI']

        # convert to str (instead of rdflib object) to avoid problems later
        self.mappings_df['subject_termtype'] = self.mappings_df['subject_termtype'].astype(str)
        self.mappings_df['object_termtype'] = self.mappings_df['object_termtype'].astype(str)

    def _remove_delimiters_from_mappings(self):
        """
        Removes demiliters from all identifiers in the mapping rules in the input DataFrame.
        """

        for i, mapping_rule in self.mappings_df.iterrows():
            self.mappings_df.at[i, 'tablename'] = _get_undelimited_identifier(mapping_rule['tablename'])
            self.mappings_df.at[i, 'subject_template'] = _get_valid_template_identifiers(
                mapping_rule['subject_template'])
            self.mappings_df.at[i, 'subject_reference'] = _get_undelimited_identifier(
                mapping_rule['subject_reference'])
            self.mappings_df.at[i, 'graph_reference'] = _get_undelimited_identifier(
                mapping_rule['graph_reference'])
            self.mappings_df.at[i, 'graph_template'] = _get_valid_template_identifiers(
                mapping_rule['graph_template'])
            self.mappings_df.at[i, 'predicate_template'] = _get_valid_template_identifiers(
                mapping_rule['predicate_template'])
            self.mappings_df.at[i, 'predicate_reference'] = _get_undelimited_identifier(
                mapping_rule['predicate_reference'])
            self.mappings_df.at[i, 'object_template'] = _get_valid_template_identifiers(
                mapping_rule['object_template'])
            self.mappings_df.at[i, 'object_reference'] = _get_undelimited_identifier(
                mapping_rule['object_reference'])

            if mapping_rule['join_conditions']:
                join_conditions = eval(mapping_rule['join_conditions'])
                for key, value in join_conditions.items():
                    join_conditions[key]['child_value'] = _get_undelimited_identifier(
                        join_conditions[key]['child_value'])
                    join_conditions[key]['parent_value'] = _get_undelimited_identifier(
                        join_conditions[key]['parent_value'])
                    self.mappings_df.at[i, 'join_conditions'] = str(join_conditions)

    def _infer_datatypes(self):
        """
        Get RDF datatypes for rules corresponding to relational data sources if they are not overridden in the mapping
        rules. The inferring of RDF datatypes is defined in R2RML specification
        (https://www.w3.org/2001/sw/rdb2rdf/r2rml/#natural-mapping).
        """

        # return if datatype inferring is not enabled in the config
        if not self.config.getboolean('CONFIGURATION', 'infer_datatypes'):
            return

        # TODO: review function and reduce the number of indentations
        raise

        for i, mapping_rule in self.mappings_df.iterrows():
            # datatype inferring only applies to relational data sources
            if mapping_rule['source_type'] in constants.VALID_ARGUMENTS['relational_source_type']:
                # datatype inferring only applies to literals
                if mapping_rule['object_termtype'] == constants.R2RML['literal']:
                    # if the literal has a language tag or an overridden datatype, datatype inference does not apply
                    if pd.isna(mapping_rule['object_datatype']) and pd.isna(mapping_rule['object_language']):

                        if pd.notna(mapping_rule['tablename']):
                            data_type = relational_source.get_column_datatype(
                                self.config, mapping_rule['source_name'], mapping_rule['tablename'],
                                mapping_rule['object_reference']
                            )

                            self.mappings_df.at[i, 'object_datatype'] = data_type
                            if data_type:
                                logging.debug("`" + data_type + "` datatype inferred for column `" +
                                              mapping_rule['object_reference'] + "` of table `" +
                                              mapping_rule['tablename'] + "` in data source `" +
                                              mapping_rule['source_name'] + "`.")

                        elif pd.notna(mapping_rule['query']):
                            # if mapping rule has a query, get the table names
                            table_names = sql_metadata.get_query_tables(mapping_rule['query'])
                            for table_name in table_names:
                                # for each table in the query check get the datatype of the object reference in that
                                # table if an exception is thrown, then the reference is no a column in that table,
                                # and nothing is done
                                try:
                                    data_type = relational_source.get_column_datatype(
                                        self.config, mapping_rule['source_name'], table_name,
                                        mapping_rule['object_reference']
                                    )

                                    self.mappings_df.at[i, 'object_datatype'] = data_type
                                    if data_type:
                                        logging.debug("`" + data_type + "` datatype inferred for reference `" +
                                                      mapping_rule['object_reference'] + "` in query (" +
                                                      mapping_rule['query'] + ") in data source `" +
                                                      mapping_rule['source_name'] + "`.")

                                    # already found it, end looping
                                    break
                                except:
                                    pass
