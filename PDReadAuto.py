import pandas as pd
import unidecode
import collections
import charset_normalizer
import chardet

def read_file(
    path,
    encoding=None,
    sep=None,
    engine=None,
    dicts=None,
    sheets=None,
    **kwargs,
) -> list:
    """
    Args:
        path: Path,
        encoding: str,
        sep: str,
        engine: str,
        dicts: dict,
        sheets: list,
    Returns:
        list of tuples [(dict_name, DataFrame)]
    """
    data_locations = []
    print(f"Opening file: {path}")
    if ".xl" in path.lower():
        if path.lower().endswith(".xls"):
            engine = None
        if path.lower().endswith(".xlsb"):
            engine = 'pyxlsb'
        data_locations = find_data_xl(path=path, engine=engine, dicts=dicts, sheets=sheets)
        if data_locations:
            return xls_to_df(path, sep, dicts=dicts, data_locations=data_locations)
        else:
            print('Opening file to Dataframe failed, find_data_xl returned empty')
            return [(None, pd.DataFrame())]

    else:
        if encoding is None: encoding = get_encoding(path)
        if sep is None: sep = get_separator(path, encoding=encoding)
        data_locations = find_data_csv(path=path, encoding=encoding, sep=sep, dicts=dicts)

        if data_locations:
            return csv_to_df(path, encoding=encoding, sep=sep, dicts=dicts, data_locations=data_locations)
        else:
            print('Opening file to Dataframe failed, find_data_csv returned empty')
            return [(None, pd.DataFrame())]

    return [(None, pd.DataFrame())]

def check_colums_match(df, dicts, sheet_name=None) -> dict:
    match_counter = {}
    # If there is a duplidate column name it will be renamed as column_name(1), column_name(2)
    df = normalize_df_as_unique_col_names(df)

    for version in dicts:
        print(f"\tDict version: '{version}' ->", end=" ")
        dict_columns = dicts.get(version).get('columns')
        df = df.map(unidecode_and_clean)
        df['_counter'] = 0
        df['_prop'] = 0
        error_cols = []

        for k, v in dict_columns.items():
            v = [unidecode_and_clean(_) for _ in v ]
            expression = ' <-AND-> '.join(v)
            search = df.apply(lambda col: any([(col.eq(dict_val)).any() for dict_val in v]), axis=1)
            if len(search) != 0:
                df['_counter'] = df['_counter'] + search
            #Check if all columns were translated
            if not any(search):
                error_cols.append(f"[{k}: {'|'.join(v)}], string searched in df: '{expression}'")

        #Prop of columns found
        df['_prop'] = df['_counter'] / len(dict_columns)

        if len(df) == 0:
            match_counter[version] = {
                "prop" : 0,
                "skiprows" : 'N/A',
                "sheet_name": sheet_name
            }
        else:
            match_counter[version] = {
                "prop" : df['_prop'].max(),
                "skiprows" : df['_prop'].idxmax(skipna=False),
                "sheet_name": sheet_name
            }
        print(f"Columns match: { round((match_counter[version].get('prop')) * 100, 2) }%")
        if error_cols:
            print(f"\t\tColumns without any match with dict '{version}' (strings already clean and padronized):")
            print('\n'.join(['\t\t\x1b[31;5m{}\x1b[0m'.format(col) for col in error_cols]))
    return match_counter

def unidecode_and_clean(x) -> str:
    x = unidecode.unidecode(str(x).lower().replace('\\n', ' ').replace('\n', ' ').strip())
    return " ".join(x.split())

def normalize_df_as_unique_col_names(df) -> pd.DataFrame:
    df = df.astype(str)
    for i in range(len(df)):
        col_names = df.iloc[i].values
        col_duplicateds = [k for k,v in collections.Counter(col_names).items() if v>1]
        for duplicated in col_duplicateds:
            counter = 0
            for j in range(len(col_names)):
                if col_names[j] == duplicated:
                    # print(f"\tPossible duplicated column '{duplicated}' renamed: ", end='')
                    # print(f"'{duplicated}({counter})'")
                    col_names[j] = duplicated+f"({counter})"
                    counter +=1
        df.iloc[[i]] = col_names
    return df

def normalize_cols_as_unique_col_names(df) -> pd.DataFrame:
    # Check if file has columns with the same name
    if not df.loc[:,df.columns.duplicated()].empty:
        print("\t\x1b[38;5;3m{}\x1b[0m".format("Warning, there are duplicated columns in file. Check dict for the next column names:"))
        # print("\t"+", ".join(df.loc[:,df.columns.duplicated()].columns.tolist()))

        col_names = df.columns.to_list()
        col_duplicateds = df.loc[:,df.columns.duplicated()].columns.to_list()

        for duplicated in col_duplicateds:
            counter = 0
            new_col_names = []
            print(f"\tRenaming ocurrencies of '{duplicated}' to ", end='')
            for i in range(len(col_names)):
                if col_names[i] == duplicated:
                    col_names[i] = duplicated+f"({counter})"
                    new_col_names.append("'"+ duplicated+f"({counter})" +"'")
                    counter +=1
            print(' AND '.join(new_col_names))
        df.columns = col_names
    return df

def columns(df, *args, **kwargs) -> pd.DataFrame:
    """
    """
    column_dict = kwargs.get("column_dict", {})
    invert = kwargs.get("invert", True)

    df.columns = [unidecode_and_clean(_) for _ in df.columns ]
    df = normalize_cols_as_unique_col_names(df)

    #BKP ROW
    df['row_data'] = df.to_json(orient='records', lines=True, force_ascii=False).splitlines()

    if invert:
        column_dict_inverted = {}
        for k, v in column_dict.items():
            v = [unidecode_and_clean(_) for _ in v ]
            for vv in v:
                column_dict_inverted[vv] = k
    else:
        column_dict_inverted = column_dict

    cols_to_stay = list(column_dict_inverted)
    cols_to_stay.append('row_data')

    df = df.drop(columns=df.columns.difference(list(cols_to_stay)), axis=1)
    df = df.rename(columns=column_dict_inverted)
    return df

def get_encoding(
        path, 
        cp_isolation=["latin_1", "iso8859_1", "iso8859_2", 'ISO-8859-1'],
        cp_exclusion=["ascii", "cp1250", "utf-8"]
    ) -> str:
    """
    Returns the encoding of file
    Args:
        path: str, path to file
        cp_isolation: list, list of probably right encodings that limits the search (used by charset-normalizer).
        cp_exclusion: list, list of wrong guessed encodings. see more in: https://github.com/Ousret/charset_normalizer/blob/master/charset_normalizer/constant.py
    Returns:
        str, identified encoding
    """
    
    file = open(path, "rb")
    read_bytes = file.read(1024*10)
    file.close()

    encodingdet = charset_normalizer.from_bytes(
        read_bytes,
        cp_isolation=cp_isolation,
        cp_exclusion=cp_exclusion
    ).best()
    # If encodign is not found then try with chardet
    if encodingdet is not None:
        encoding = encodingdet.encoding
        confidence = encodingdet.coherence
    else:
        print("\t\x1b[38;5;3m{}\x1b[0m".format("charset-normalizer failed: trying with chardet"))
        encodingdet = chardet.detect(read_bytes)
        if encodingdet.get('encoding') == 'ascii':
            # ASCII is not a good encoding choice, using superset latin1 to replace it
            encoding = 'latin1'
        else:
            encoding = encodingdet.get('encoding')
        confidence = encodingdet.get('confidence')

    assert confidence > 0.5, f"""
    get_encoding: confidence: {confidence} not possible to detect encoding with confidence > 0.5 in: {path}
    check cp_isolation and cp_exclusion in PDReadAuto -> get_encoding
    """

    print("\033[0;32m{}\x1b[0m".format(f"get_encoding: detected encoding: {encoding} - confidence: {confidence}"))
    return encoding

def get_separator(
        path, 
        encoding="utf-8",
        delimiters = ['#', '|', ';', '\t', ':', ',', '^']
    ) -> str:
    """
    Returns the separator of csv/txt file
    Args:
        path: str
        encoding: str (default UTF-8)
        delimiters: list, list of possible column delimiters
    Returns:
        str
    """
    sep = None
    file = open(path, "r", encoding=encoding)
    data = file.read(2048)
    file.close()
    data_counter = collections.Counter(data)

    # Count ocurrencies of each possible delimiter 
    sep_counter = {}
    for sep in delimiters:
        sep_counter[sep] = data_counter[sep]

    # Returns the best result
    sep = max(sep_counter, key=sep_counter.get)

    assert sep is not None, f"get_separator: separator not detected. Separator is out of search list: {delimiters} or separator is the type 'fixed'"

    print("\033[0;32m{}\x1b[0m".format(f"get_separator: separator detected: {repr(sep)}"))
    return sep

def find_data_xl(path, engine=None, sep=None, dicts=None, sheets=None) -> list:
    best_props = []
    xl=None
    
    # Trying to open .xls file, if importing textbox show when opening filem, then the file must be imported and saved as another extension.
    exp_msg=''
    try:
        xl = pd.ExcelFile(path, engine=engine)
    except Exception as e:
        print("Problem found when trying to open file:")
        exp_msg = str(e)

    assert xl is not None, f"""
    Check if file can be opened in your PC, if file shows import dialog when opening (.xls), then it must be imported and saved as new file.
    Exception:
    {exp_msg}
    """

    sheets_to_look = xl.sheet_names if sheets is None else sheets

    for sheet_name in sheets_to_look:
        test = pd.read_excel(
            path, 
            dtype=str,
            header=None,
            #For big files keep nrwos low. If header not in 50 first rows then you will need to change that.
            nrows=50,
            sheet_name=sheet_name
        )
        print('-------------------------------------------------------------------')
        print(f"Checking dicts match in sheet -> '{sheet_name}'")
        col_matchs = check_colums_match(df=test, dicts=dicts, sheet_name=sheet_name)

        # Return best result
        best_cfg = max(col_matchs, key=lambda x: col_matchs[x].get('prop',0))
        best_prop = col_matchs.get(best_cfg).get('prop')

        # Results with match < 70% are discarded
        if best_prop < 0.7:
            print('\t\x1b[31;5m{}\x1b[0m'.format(f"Ignoring sheet: '{sheet_name}'. Low correlation with columns in specified dicts (match < 70%)"))
        else:
            print('\t\033[0;32m{}\x1b[0m'.format(f"Best result: dict '{best_cfg}' with correlation {round(best_prop * 100, 2)}% of columns in sheet '{sheet_name}'"))
            best_props.append({best_cfg : col_matchs.get(best_cfg)})
    print('-------------------------------------------------------------------')

    if best_props:
        print('Best result of each sheet_name:')
        for dict_bp in best_props:
            for bp in dict_bp:
                print("\t\033[0;32m{}\x1b[0m".format(f"Dict: '{bp}' -> match: { round(dict_bp.get(bp).get('prop') * 100, 2) }% in line: { dict_bp.get(bp).get('skiprows') } sheet: '{ dict_bp.get(bp).get('sheet_name') }'"))
    else: print('\x1b[31;5m{}\x1b[0m'.format(f"Its not possible to open file, columns dont match > 70% in the sheets found"))

    return best_props

def xls_to_df(path, engine=None, sep=',', dicts=None, data_locations=None) -> list:
    print('-------------------------------------------------------------------')
    print("Transforming sheets to Pandas DataFrame:")
    dfs = []
    for data_location in data_locations:
        for version in data_location:
            skiprows = data_location.get(version).get('skiprows')
            sheet_name = data_location.get(version).get('sheet_name')
            print(f"Opening sheet '{sheet_name}' with dict '{version}'")
            df = pd.read_excel(
                path, 
                engine=engine,     
                sheet_name=sheet_name, 
                skiprows=skiprows,
                dtype=str,
                header=None,
                # nrows=1000, -> Test with big file
            )
            df = df.rename(columns=df.iloc[0]).drop(df.index[0])
            df = columns(df, column_dict=dicts.get(version)["columns"])
            df.drop(columns=["_ignore_"], inplace=True, errors='ignore')

            df = df.reset_index(drop=True)
            df["data_source"] = path+ " {" + f"sheet: {sheet_name}, version: {version}" + "}"
            dfs.append((version, df))
    print("Finished!")
    return dfs

def find_data_csv(path, encoding=None, sep=None, dicts=None) -> list:
    print('-------------------------------------------------------------------')
    with open(path, 'r', encoding=encoding) as temp_f:
        col_count = [ len(l.split(sep)) for l in temp_f.readlines(2048) ]
    #Generate name for columns (0, 1, 2, ..., maximum columns - 1)
    column_names = [i for i in range(0, max(col_count))]

    best_props = []
    test = pd.read_csv(
        path, 
        encoding=encoding, 
        sep=sep, 
        dtype=str,
        header=None,
        nrows=50,
        names=column_names,
        skip_blank_lines=False,
    )
    print(f"Checking dicts match in file")
    col_matchs = check_colums_match(df=test, dicts=dicts)

    for cm in col_matchs:
        print(f'\t{ cm } -> match: { round(col_matchs.get(cm).get("prop") * 100, 2) }% in line: { col_matchs.get(cm).get("skiprows")+1 } ')
    
    #Pega o melhor resultado
    best_cfg = max(col_matchs, key=lambda x: col_matchs[x].get('prop',0))
    best_prop = col_matchs.get(best_cfg).get('prop')

    #Se for < 70% descarta
    print("-------------------------------------------------------------------")
    if best_prop < 0.7:
        print('\x1b[31;5m{}\x1b[0m'.format(f"Ignoring file: Low correlation with columns in specified dicts (match < 70%)"))
    else:
        print("\033[0;32m{}\x1b[0m".format(f"Best result: {best_cfg} with correlation {round(best_prop * 100, 2)}% of columns"))
        best_props.append({best_cfg : col_matchs.get(best_cfg)})

    return best_props

def csv_to_df(path, encoding=None, sep=',', dicts=None, data_locations=None) -> list:
    print("Transforming file to Pandas DataFrame")
    with open(path, 'r', encoding=encoding) as temp_f:
        col_count = [ len(l.split(sep)) for l in temp_f.readlines(2048) ]
    #Generate name for columns (0, 1, 2, ..., maximum columns - 1)
    column_names = [i for i in range(0, max(col_count))]

    dfs = []
    for data_location in data_locations:
        for version in data_location:
            skiprows = data_location.get(version).get('skiprows')
            sheet_name = data_location.get(version).get('sheet_name')
            df = pd.read_csv(
                path, 
                encoding=encoding, 
                sep=sep, 
                dtype=str,
                skiprows=skiprows,
                engine='python',
                header=None,
                names=column_names,
                skip_blank_lines=False,
                # nrows=1000,
            )
            df = df.rename(columns=df.iloc[0]).drop(df.index[0])
            df = columns(df, column_dict=dicts.get(version)["columns"])
            df.drop(columns=["_ignore_"], inplace=True, errors='ignore')

            df = df.reset_index(drop=True)
            df["data_source"] = path + " {" + f"version: {version}" + "}"

            dfs.append((version, df))
    print("Finished!")
    return dfs