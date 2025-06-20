import xml.etree.ElementTree as ET
from difflib import get_close_matches

def clean_query(query):
    """Clean and format SQL query string."""
    if not query:
        return None
    # Remove surrounding quotes if present
    if query.startswith('"') and query.endswith('"'):
        query = query[1:-1]
    # Replace escaped quotes and newlines
    query = query.replace('\\"', '"').replace('\\n', '\n')
    return query

def extract_query_for_component(comp, comp_type, params):
    """Extract SQL query based on component type."""
    query = None
    queries = []
    
    # Common SQL parameter names
    sql_param_names = [
        "QUERY", "Sql Query", "QUERYSTORE", "SQLQUERY",
        "WHERE", "OTHER_SQL", "SQL", "SCRIPT", "SOURCE_SQL",
        "TARGET_SQL", "MERGE_SQL", "CUSTOM_SQL", "PROPERTIES"
    ]
    
    def clean_sql_text(sql_text: str) -> str:
        """Clean SQL text by handling line endings and formatting."""
        # Replace various line ending combinations with standard newline
        sql_text = str(sql_text).replace('\\r\\n', '\n')
        sql_text = sql_text.replace('\\n', '\n')
        sql_text = sql_text.replace('\\r', '\n')
        sql_text = sql_text.replace('\r\n', '\n')
        sql_text = sql_text.replace('\r', '\n')
        
        # Clean up other escaped characters
        sql_text = sql_text.replace('\\"', '"')
        sql_text = sql_text.replace('\\t', '    ')  # Replace tabs with spaces
        
        # Split into lines, strip whitespace, and rejoin
        lines = [line.strip() for line in sql_text.split('\n')]
        return '\n'.join(line for line in lines if line)
    
    # First try common SQL parameters
    for param_name in sql_param_names:
        if param_name in params:
            param_query = clean_query(params[param_name])
            if param_query:
                # For PROPERTIES parameter, try to extract SQL from JSON-like structure
                if param_name == "PROPERTIES":
                    try:
                        # Look for SQL patterns in the PROPERTIES value
                        sql_patterns = {
                            "MERGE INTO": [";", '",', '"}'],
                            "CREATE TABLE": [";", '",', '"}'],
                            "SELECT": [";", '",', '"}'],
                            "INSERT INTO": [";", '",', '"}'],
                            "UPDATE": [";", '",', '"}'],
                            "DELETE FROM": [";", '",', '"}']
                        }
                        
                        # Find the start of any SQL pattern
                        for pattern, end_markers in sql_patterns.items():
                            sql_start = str(param_query).find(pattern)
                            if sql_start != -1:
                                # For MERGE statements, we need to find the last semicolon
                                if pattern == "MERGE INTO":
                                    # Find all semicolons after the MERGE
                                    current_pos = sql_start
                                    last_semicolon = -1
                                    while True:
                                        next_semicolon = str(param_query).find(';', current_pos + 1)
                                        if next_semicolon == -1:
                                            break
                                        section = str(param_query)[current_pos:next_semicolon]
                                        if '"}' in section or '","' in section or any(p in str(section).upper() for p in sql_patterns.keys() if p != pattern):
                                            break
                                        last_semicolon = next_semicolon
                                        current_pos = next_semicolon
                                    
                                    if last_semicolon != -1:
                                        sql_end = last_semicolon + 1
                                    else:
                                        possible_ends = [str(param_query).find(marker, sql_start) for marker in end_markers]
                                        valid_ends = [end for end in possible_ends if end != -1]
                                        sql_end = min(valid_ends) if valid_ends else len(param_query)
                                else:
                                    possible_ends = [str(param_query).find(marker, sql_start) for marker in end_markers]
                                    valid_ends = [end for end in possible_ends if end != -1]
                                    sql_end = min(valid_ends) if valid_ends else len(param_query)
                                
                                extracted_query = str(param_query)[sql_start:sql_end].strip()
                                if extracted_query:
                                    # Clean up the extracted query
                                    extracted_query = clean_sql_text(extracted_query)
                                    queries.append(f"-- {param_name} Query:\n{extracted_query}")
                                break
                    except Exception as e:
                        print(f"Error extracting query from PROPERTIES: {e}")
                else:
                    clean_query_text = clean_sql_text(param_query)
                    queries.append(f"-- {param_name}:\n{clean_query_text}")
    
    # Extract queries from properties/queries section
    for queries_elem in comp.findall(".//queries"):
        for query_elem in queries_elem.findall("query"):
            query_id = query_elem.attrib.get("id", "Unknown")
            query_text = clean_query(query_elem.text)
            if query_text:
                queries.append(f"-- Query {query_id}:\n{query_text}")
    
    # Look for merge queries in properties
    for properties in comp.findall(".//properties"):
        for property_elem in properties.findall("property"):
            prop_name = property_elem.attrib.get("name", "").lower()
            if "merge" in prop_name or "query" in prop_name:
                for value in property_elem.findall("value"):
                    query_text = clean_query(value.text)
                    if query_text:
                        queries.append(f"-- {property_elem.attrib.get('name')}:\n{query_text}")
    
    # Component-specific handling
    if not queries:  # Only proceed if no queries found yet
        if comp_type.startswith(("tDB", "tSnowflake", "tJDBC", "tOracle", "tMysql", "tPostgresql")):
            # Database components might have additional SQL-related parameters
            sql_related_params = []
            
            # Check for WHERE clause
            where_clause = clean_query(params.get("WHERE"))
            if where_clause:
                sql_related_params.append(f"-- WHERE Clause:\n{where_clause}")
            
            # Check for JOIN conditions
            join_conditions = clean_query(params.get("JOIN"))
            if join_conditions:
                sql_related_params.append(f"-- JOIN Conditions:\n{join_conditions}")
            
            # Check for GROUP BY
            group_by = clean_query(params.get("GROUP_BY"))
            if group_by:
                sql_related_params.append(f"-- GROUP BY:\n{group_by}")
            
            # Look for any SQL-related parameters
            for param in comp.findall("elementParameter"):
                param_name = param.attrib.get("name", "").lower()
                if any(sql_term in param_name for sql_term in ["sql", "query", "script", "where", "join", "merge"]):
                    param_value = clean_query(param.attrib.get("value"))
                    if param_value:
                        sql_related_params.append(f"-- {param.attrib.get('name')}:\n{param_value}")
            
            if sql_related_params:
                queries.extend(sql_related_params)
                
        elif comp_type == "tMap":
            # For tMap, extract SQL expressions and filters
            expressions = []
            
            # Extract table joins and filters
            for table in comp.findall(".//table"):
                table_name = table.attrib.get("name", "")
                
                # Extract join conditions
                join_conditions = table.findall(".//joinConditions/joinCondition")
                if join_conditions:
                    join_exprs = []
                    for jc in join_conditions:
                        join_exprs.append(jc.attrib.get("expression", ""))
                    if join_exprs:
                        expressions.append(f"-- {table_name} Join Conditions:\n" + "\nAND ".join(join_exprs))
                
                # Extract filters
                filters = table.findall(".//filterConditions/filterCondition")
                if filters:
                    filter_exprs = []
                    for fc in filters:
                        filter_exprs.append(fc.attrib.get("expression", ""))
                    if filter_exprs:
                        expressions.append(f"-- {table_name} Filters:\n" + "\nAND ".join(filter_exprs))
                
                # Extract column expressions
                for column in table.findall(".//column"):
                    expression = column.attrib.get("expression", "")
                    if "SELECT" in expression.upper() or "FROM" in expression.upper():
                        expressions.append(f"-- {table_name}.{column.attrib.get('name', 'Unknown Column')}:\n{expression}")
            
            if expressions:
                queries.extend(expressions)
                
        elif comp_type == "tSQLTemplate":
            # Extract SQL template content
            template = clean_query(params.get("SQLTEMPLATE"))
            parameters = []
            
            # Extract template parameters
            for param in comp.findall(".//sqlPatternValue"):
                param_name = param.attrib.get("name", "")
                param_value = param.attrib.get("value", "")
                if param_name and param_value:
                    parameters.append(f"-- Parameter {param_name}:\n{param_value}")
            
            if template:
                parameters.insert(0, f"-- SQL Template:\n{template}")
                queries.extend(parameters)
    
    # Combine all queries with proper spacing
    return "\n\n".join(queries) if queries else None

def get_component_category(comp_type):
    """
    Get the high-level category for a component type.
    Only used for displaying in the component details sidebar.
    """
    # Database components
    if any(comp_type.startswith(prefix) for prefix in ['tSnowflake', 'tOracle', 'tMysql', 'tPostgresql', 'tDB']):
        return 'Database Component'
    
    # Map components
    elif comp_type.startswith('tMap'):
        return 'Data Mapping Component'
    
    # File components
    elif any(word in comp_type for word in ['File', 'CSV', 'Excel', 'XML']):
        return 'File Component'
    
    return 'Other Component'

def map_component_type(unique_name, comp_type, params):
    """
    Intelligently map component types based on patterns and parameters.
    """
    # Common Talend component mappings
    known_mappings = {
        'tDBConnection': {
            'snowflake': 'tSnowflakeConnection',
            'oracle': 'tOracleConnection',
            'mysql': 'tMysqlConnection',
            'postgresql': 'tPostgresqlConnection'
        },
        'tDBRow': {
            'snowflake': 'tSnowflakeRow',
            'oracle': 'tOracleRow',
            'mysql': 'tMysqlRow',
            'postgresql': 'tPostgresqlRow'
        }
    }

    # If we already have a valid component type, return it
    if comp_type and comp_type != "Unknown" and comp_type != "UNKNOWN":
        return comp_type

    # Try to extract type from UNIQUE_NAME
    if unique_name:
        base_type = unique_name.split('_')[0]  # e.g., tDBRow from tDBRow_1
        if base_type in known_mappings:
            # Look for database type hints in parameters
            db_type = None
            for param_name, param_value in params.items():
                param_value = str(param_value).lower()
                if "snowflake" in param_value:
                    db_type = "snowflake"
                    break
                elif "oracle" in param_value:
                    db_type = "oracle"
                    break
                elif "mysql" in param_value:
                    db_type = "mysql"
                    break
                elif "postgresql" in param_value or "postgres" in param_value:
                    db_type = "postgresql"
                    break
            
            if db_type and db_type in known_mappings[base_type]:
                return known_mappings[base_type][db_type]
            
            # If no specific DB type found but it's a known base type,
            # use Snowflake as default for tDBRow and tDBConnection
            if base_type in ['tDBRow', 'tDBConnection']:
                return known_mappings[base_type]['snowflake']

    return comp_type or "Unknown"

def extract_generic_and_safe(xml_path):
    tree = ET.parse(xml_path)
    root = tree.getroot()

    nodes = []
    real_edges = []
    used_ids = set()
    unique_name_to_uid = {}
    component_registry = {}
    source_ids = []
    target_ids = []
    x, y_spacing = 120, 120

    def unique_id(base):
        counter = 1
        while f"{base}_{counter}" in used_ids:
            counter += 1
        new_id = f"{base}_{counter}"
        used_ids.add(new_id)
        return new_id

    print("\n=== 🔍 Parsing Components ===")
    for idx, comp in enumerate(root.findall(".//node")):
        # Get the component type from componentName attribute
        comp_type = comp.attrib.get("componentName", "")
        if not comp_type or comp_type == "UNKNOWN":
            # Try to get it from COMPONENT_NAME parameter if componentName is not available
            for param in comp.findall("elementParameter"):
                if param.attrib.get("name") == "COMPONENT_NAME":
                    comp_type = param.attrib.get("value", "")
                    break
        
        # Clean up the component type
        comp_type = comp_type.strip() if comp_type else "Unknown"
        
        label = comp.attrib.get("label", "").strip()
        name = comp.attrib.get("name", "").strip()
        is_activate = comp.attrib.get("isActivate", "true").strip().lower()

        # Extract UNIQUE_NAME
        unique_name = None
        for param in comp.findall("elementParameter"):
            if param.attrib.get("name") == "UNIQUE_NAME":
                unique_name = param.attrib.get("value")
                break

        # Get all parameters for component type mapping
        params = {p.attrib.get("name"): p.attrib.get("value") for p in comp.findall("elementParameter")}
        
        # Get the technical component type
        technical_type = map_component_type(unique_name, comp_type, params)
        
        # Get the component category for display in sidebar
        display_type = get_component_category(technical_type)

        display_label = label or name or technical_type or "Unknown"
        base_id = f"{technical_type}_{display_label}".replace(" ", "_")
        uid = unique_id(base_id)

        if unique_name:
            unique_name_to_uid[unique_name] = uid

        # Enhanced SQL Query detection
        query = extract_query_for_component(comp, technical_type, params)

        nodes.append({
            "id": uid,
            "data": {
                "label": display_label,
                "status": "active",  # Will be updated later
                "sql": query,
                "component_type": display_type,  # For sidebar display
                "technical_type": technical_type  # For internal use
            },
            "position": {"x": x, "y": 100 + idx * y_spacing}
        })

        component_registry[uid] = {
            "id": uid,
            "unique_name": unique_name,
            "label": display_label,
            "technical_type": technical_type,
            "component_type": display_type,
            "is_activate": is_activate,
            "sql": query,
            "position": {"x": x, "y": 100 + idx * y_spacing}
        }

    print("\n=== 🔗 Resolving Connections Using UNIQUE_NAME ===")
    for conn in root.findall(".//connection"):
        src = conn.attrib.get("source")
        tgt = conn.attrib.get("target")
        label = conn.attrib.get("connectorName", "SUBJOB_OK")

        src_uid = unique_name_to_uid.get(src)
        tgt_uid = unique_name_to_uid.get(tgt)

        print(f"🔍 Connection: {src} → {tgt} → {src_uid} → {tgt_uid}")

        if src_uid and tgt_uid:
            real_edges.append({
                "id": f"{src_uid}-{tgt_uid}",
                "source": src_uid,
                "target": tgt_uid,
                "label": label
            })
            source_ids.append(src_uid)
            target_ids.append(tgt_uid)

    print("\n=== ✅ Classifying Component Status ===")
    for node in nodes:
        uid = node["id"]
        comp = component_registry[uid]
        if comp["is_activate"] == "false":
            status = "deactivated"
        elif uid in source_ids or uid in target_ids:
            status = "active"
        else:
            status = "inactive"

        print(f"  ⬛ {comp['label']} ({comp['unique_name']}): {status}")
        node["data"]["status"] = status

    return nodes, real_edges, real_edges
