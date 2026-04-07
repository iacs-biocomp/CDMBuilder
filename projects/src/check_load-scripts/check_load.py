import glob
import json
import logging
import os
import re
import duckdb
import pandas as pd


# ===============================================================
# Script Compatibility Notice
# ---------------------------------------------------------------
# This script requires DuckDB version 1.0.0 or higher.
# Older versions may not support the functions used (e.g., sniff_csv).
# Please ensure your DuckDB installation is up to date.
# ===============================================================

def read_file(entity_structure, dtype_, parse_dates):
    logging.info("Trying to read the file with the configuration provided.")
    try:
        df = pd.read_csv(
            entity_structure['uploaded_filename'],
            sep=entity_structure['separator'],
            dtype=dtype_,
            parse_dates=parse_dates,
            encoding=entity_structure['encoding']
        )
        df = df[entity_structure['entity_variables']]  
        len_df = len(df)
        logging.info(f"{len_df} records read.")
        return df
    except ValueError as e:
        logging.error(f"Reading the file with the provided configuration failed!")
        logging.error(str(e))
        df = pd.read_csv(
            entity_structure['uploaded_filename'],
            sep=entity_structure['separator'],
            encoding=entity_structure['encoding']
        )
        variables_name = []
        variables_format = []
        for k, v in dtype_.items():
            if pd.StringDtype != type(v):
                variables_name.append(k)
                variables_format.append(v)
        for v in df.dtypes.items():
            logging.error(f"{v}")
        exit(1)


def load_file(entity_structure, df):
    logging.info(f"Trying to connect to the database ...")
    try:
        con = duckdb.connect(database_path, read_only=False)
        con.execute("SET GLOBAL pandas_analyze_sample=500000")
        logging.info(f"Connected!")
        entity_name_ = entity_structure['entity_name']
        logging.info(f"Trying to load records in the table \"{entity_name_}\"")
        query = "INSERT INTO {entity} SELECT * FROM df;".format(entity=entity_name_)
        con.execute(query)
        logging.info(f"{entity_structure['uploaded_filename']} -> LOADED!")
        global entities_uploaded
        entities_uploaded = entities_uploaded + 1
    except Exception as e:
        logging.error("Something went wrong trying to insert the data!")
        logging.error(str(e))
    finally:
        con.close()

        
def found_candidate(file_columns, entity_colums):
    return set(entity_colums) == set(file_columns) and len(entity_colums) == len(file_columns)


def query_found_errors(filename,entity_info):
    queries=[]
    for entity_key, entity_value in entity_info['parse'].items():
        query_errors = f"""select * from (select row_number() over() as rk, TRY_CAST({entity_key} AS {entity_value})::VARCHAR AS {entity_key}_error, {entity_key} 
        from read_csv('{filename}', all_varchar=TRUE)) where {entity_key}_error != {entity_key} or {entity_key}_error = {entity_key} is null"""
        queries.append({'variable':entity_key, 'query':query_errors})
    return queries

        
def sniff_original_files(uploaded_file_,entity_info_):        
    logging.info("Sniffing original files")
    con = duckdb.connect(database_path, read_only=False)
    result = con.execute(f"from sniff_csv('{uploaded_file_}', sample_size = 250000)").df()
    columns = result['Columns']
    columns = pd.DataFrame(list(columns[0]))
    #'entity_name': entity_name, 'entity_columns':entity_variables, 'parse':info 
    entities_to_upload = []
    entity_assigned = "Not found"
    for entity in entity_info_:
        candidate = found_candidate(columns['name'],entity['entity_columns'])
        if candidate:
            queries_errors = query_found_errors(uploaded_file_,entity)
            entity_assigned = entity['entity_name']           
            parse_original_file = {k: entity['parse'][k] for k in columns['name']}         
            entities_to_upload.append({"file":uploaded_file_,"entity": entity['entity_name'],"parse": parse_original_file, 'queries':queries_errors})
    
    logging.info("Original file characteristics")
    logging.info("=============================")
    logging.info(f"File: {uploaded_file_.replace('./inputs/','')}")
    logging.info(f"Delimiter: {result['Delimiter'][0]}")
    logging.info(f"Quote: {result['Quote'][0]}")
    logging.info(f"Escape: {result['Escape'][0]}")
    logging.info(f"NewLineDelimiter: {result['NewLineDelimiter'][0]}")
    logging.info(f"SkipRows: {result['SkipRows'][0]}")
    logging.info(f"HasHeader: {result['HasHeader'][0]}")
    logging.info(f"DateFormat: {result['DateFormat'][0]}")
    logging.info(f"TimestampFormat: {result['TimestampFormat'][0]}")
    logging.info('Columns with inferred types:')
    logging.info("\n" + columns.to_string(index=False))
    logging.info(f'Assigned entity: {entity_assigned}')
    logging.info("=============================\n\n")
    return entities_to_upload

def get_duckdb_parsed_config(entity_variables,entity_formats):
    dtype_ = {}
    parse_dates = []
    
    try_casting =[]
    
    for c, f in zip(entity_variables, entity_formats):
        if f == 'string':
            dtype_[c] = 'VARCHAR'
            pass
        elif f == 'boolean':
            dtype_[c] = 'BOOLEAN'
        elif f == 'date':
            dtype_[c] = 'DATE'
        elif f == 'datetime':
            dtype_[c] = 'TIMESTAMP'
        elif f == 'integer':
            dtype_[c] = 'HUGEINT'
        elif f == 'double':
            dtype_[c] = 'DOUBLE'
            pass
        else:
            logging.warning(f"Format '{f}' not found, will be interpreted as String object.")
            dtype_[c] = 'VARCHAR'
        try_casting.append(f"TRY_CAST({c} AS {dtype_[c]}) AS {c}_error")
    return dtype_
   


def get_entity_info():
    if 'entities' in configuration_file:
        entities_structure = []
        for entity in configuration_file['entities']:
            if 'name' not in entity or 'variables' not in entity:
                logging.error("properties \"name\" or \"variables\" not found in entity! Check specifications!")
                exit(1)
            entity_name = entity['name']
            logging.info(f"Processing entity \"{entity_name}\"...")
            try:
                entity_variables = [variable['label'] for variable in entity['variables']]
                entity_formats = [str(variable['format']).lower() for variable in entity['variables']]
                create_entity_table_if_not_exists(entity_name,entity_variables,entity_formats)
                info = get_duckdb_parsed_config(entity_variables,entity_formats)
                r = {'entity_name': entity_name, 'entity_columns':entity_variables,'entity_formats':entity_formats, 'parse':info}
                entities_structure.append(r)
            except Exception as e:
                logging.error("Variables must have the properties \"label\" and \"format\"")
                logging.error(str(e))
                exit(1)
        return entities_structure
    else:
        logging.error("\"entities\" not found in your configuration file! Check specifications!")
        exit(1)
 

def create_entity_table_if_not_exists(entity_name_, entity_variables_, entity_formats_):
    format_translation = {
        "string": "VARCHAR",
        "boolean": "BOOLEAN",
        "date": "DATE",
        "datetime": "TIMESTAMP",
        "integer": "HUGEINT",
        "double": "DOUBLE"
    }
    
    query = f"""CREATE TABLE IF NOT EXISTS {entity_name_}({entity_variables_[0]} {format_translation.get(entity_formats_[0], "VARCHAR")}"""
    for (variable_, format_) in zip(entity_variables_[1:], entity_formats_[1:]):
        query += f""", {variable_} {format_translation.get(format_)}"""
    query += ");"
    try:
        logging.info(f"Trying to connect to the database ...")
        logging.info(f"Trying to create the table for entity \"{entity_name_}\"")
        logging.info(f"Table structure:\n {query}")
        con = duckdb.connect(database_path, read_only=False)
        con.execute(query)
        logging.info(f"Table successfully created!")
    except Exception as e:
        logging.error("Something went wrong in the creation of the table")
        logging.error(str(e))
    finally:
        con.close()

if __name__ == '__main__':
    # Do not modify if you use the deployment container!
    os.chdir(os.path.dirname(__file__))
    logging.basicConfig(format='%(levelname)s:: %(message)s', level=logging.INFO)
    logging.info("Starting Checking data syntax process")
    # Relative paths to the work structure provided from CDMBuilder.
    # Important! The functionality of this script may vary, but the file name,
    # where it reads the data and where the results are saved must be maintained
    # for the correct functioning of the tool.
    database_path = '../../inputs/data.duckdb'
    configuration_file_path = '../../docs/CDM/cdmb_config.json'
    output_path = '../../outputs'
    upload_files_path = './inputs'
    global entities_uploaded
    entities_uploaded = 0
    # Opening JSON file
    try:
        with open(configuration_file_path, encoding='utf-8') as configuration_file:
            configuration_file = json.load(configuration_file)
    except FileNotFoundError as e:
        logging.error("Configuration file "" is missing!")
        exit(1)
    logging.info("Configuration file loaded\n")
    CDMB_VERSION = configuration_file["cdmb_version"] if "cdmb_version" in configuration_file else "Non-versioned"
    ASPIRE_VERSION = os.environ.get('ASPIRE_VERSION', 'Non-versioned')
    PIPELINE_VERSION = os.environ.get('PIPELINE_VERSION', 'Non-versioned')
    logging.info("#########################################")
    logging.info(f"# CDMB version: {CDMB_VERSION}")
    logging.info(f"# ASPIRE version: {ASPIRE_VERSION}")
    logging.info(f"# PIPELINE version: {PIPELINE_VERSION}")
    logging.info("#########################################\n")
    
    entity_info = get_entity_info()    
    csv_files = glob.glob(upload_files_path + "/*.csv", recursive=True)
    uploaded_file_structure = []
    logging.info(f"-Found {len(csv_files)} uploaded files to check and map!")
    all_files_to_upload = []
    if len(csv_files) > 0: logging.info("Starting the check of the files that do match an entity.")
    for uploaded_file in csv_files:
        try:
            entities_to_upload = sniff_original_files(uploaded_file, entity_info)
            all_files_to_upload = all_files_to_upload + entities_to_upload
            
        except Exception as e:
            logging.error(f"Something went wrong trying to read \"{uploaded_file}\" file")
            logging.error(str(e))
            exit(1)

    for entity in entity_info:
        files_to_upload = [item for item in all_files_to_upload if item["entity"] == entity['entity_name']]
        if len(files_to_upload) == 0:
            logging.warning(f"No file of the uploaded files has been found that matches the header with the "
                                f"configuration of the \"{entity['entity_name']}\" entity.!")
        else:
            logging.info(f"A file (total files: {len(files_to_upload)}) of the uploaded files has been found that matches the header with the "
                                f"configuration of the \"{entity['entity_name']}\" entity.!")
            
            try:
                logging.info(f"Trying to connect to the database ...")
                con = duckdb.connect(database_path, read_only=False)
                logging.info(f"Trying to load records in the table \"{entity['entity_name']}\"")
                has_pass = 0
                for i, item in enumerate(files_to_upload):
                    try:
                        parse_configuracion = json.dumps(item['parse'], ensure_ascii=False)
                        if i == 0:
                            # create o replace 
                            query = f"CREATE OR REPLACE TABLE {entity['entity_name']} as SELECT {','.join(entity['entity_columns'])} from (select * FROM read_csv('{item['file']}',nullstr=['NA',''],  columns ={parse_configuracion}))"
                            con.execute(query)
                            logging.info(f"Table successfully created! (CREATE OR REPLACE OPERATION)")
                        else: 
                            query = f"INSERT INTO {entity['entity_name']} SELECT {','.join(entity['entity_columns'])} from (select *  FROM read_csv('{item['file']}', nullstr=['NA',''],  columns ={parse_configuracion}))"
                            con.execute(query)
                            logging.info(f"Table successfully created! (INSERT OPERATION)")
                        has_pass = has_pass + 1
                    except Exception as e:
                        logging.error("Something went wrong trying to insert data into the database\n")
                        logging.error(str(e))
                        logging.error("\nChecking variable casting variable by variable\n")
                        for query in item['queries']:
                                 
                            
                            result_error = con.execute(query['query']).df()
                            n_errors = len(result_error)
                            if n_errors > 0:
                                logging.error("---------")
                                logging.error(f"Checking variable: {query['variable']}")
                                logging.error(f"Filename: {item['file'].replace('./inputs/','')}")
                                logging.error(f"Found casting errors in {len(result_error)} line(s)")
                                n_lines = result_error["rk"].astype(str).tolist()
                                n_wrong_values = result_error[query['variable']].astype(str).unique().tolist()
                                logging.error(f"{len(n_wrong_values)} values have been found that cause casting errors.")
                                n_lines_st = ""
                                n_values_st = ""
                                if len(n_lines) > 15:
                                    n_lines_st = ", ".join(f"'{v}'" for v in n_lines[:15]) + ", ..."
                                else:
                                    n_lines_st = ", ".join(f"'{v}'" for v in n_lines)
                                
                                if len(n_wrong_values) > 15:
                                    n_values_st = ", ".join(f"'{v}'" for v in n_wrong_values[:15]) + ", ..."
                                else:
                                    n_values_st = ", ".join(f"'{v}'" for v in n_wrong_values)
                                    
                                logging.error(f"Line number where errors were found: {n_lines_st}")
                                logging.error(f"Example of erroneous values found: {n_values_st}")        
                                logging.error("---------")
              
                
                
                if has_pass == len(files_to_upload):
                    entities_uploaded = entities_uploaded+1
                    result = con.execute(f"select count(*) as n_rows from {entity['entity_name']}").df()            
                    logging.info(f"{result.loc[0, 'n_rows']} records read.\n\n")
                elif has_pass == 0:
                    logging.error("0 records read")
                else:
                    result = con.execute(f"select count(*) as n_rows from {entity['entity_name']}").df()     
                    logging.info(f"The entity has not been fully loaded with all records.")       
                    logging.info(f"{result.loc[0, 'n_rows']} records read.\n\n")
                
            except Exception as e :
                logging.error("Something went wrong trying to connect to the database")
                logging.error(str(e))
   
            finally:
                con.close()
    if entities_uploaded == 0:
            logging.error("None of the uploaded files have been matched with an entity!")
    else:
        logging.info(f"Total number of entities that have been matched to an uploaded file: {entities_uploaded}")

        
