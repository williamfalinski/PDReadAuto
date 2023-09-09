import pandas as pd
import unidecode
import collections

def read_file(
    path,
    encoding=None,
    sep=None,
    engine=None,
    dicts=None,
    **kwargs,
):
    """
    Args:
        path: Path
        encoding: str
        sep: str
        engine: str (default None)
        dicts: dict
    
    Returns:
        pandas.DataFrame
    """
    data_locations = []
    if ".xl" in path.lower():
        if path.lower().endswith(".xls"):
            engine = None
        if path.lower().endswith(".xlsb"):
            engine = 'pyxlsb'
        
        data_locations = find_data_xl(path=path, engine=engine, dicts=dicts)
        if data_locations:
            return xls_to_df(path, sep, dicts=dicts, data_locations=data_locations)
        else:
            print('Não foi possível abrir o arquivo, find_data_xl retornou vazio')
            return [(None, pd.DataFrame())]

    # else:
    #     if encoding is None: encoding = get_encoding(path)
        # if sep is None: sep = get_separator(path, encoding=encoding)

        # data_locations = find_data_csv(path=path, encoding=encoding, sep=sep, dicts=dicts)

        # if data_locations:
        #     return csv_to_df(path, encoding=encoding, sep=sep, dicts=dicts, data_locations=data_locations)
        # else:
        #     print('Não foi possível abrir o arquivo, find_data_csv retornou vazio')
        #     return [(None, pd.DataFrame())]

    return [(None, pd.DataFrame())]



def find_data_xl(path, engine=None, sep=None, dicts=None):
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

    for sheet_name in xl.sheet_names:
        test = pd.read_excel(
            path, 
            dtype=str,
            header=None,
            #For big files keep nrwos low. If header not in 50 first rows then you will need to change that.
            nrows=50,
            sheet_name=sheet_name
        )
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
        print('===================================================')

    if best_props:
        print('Best result of each sheet_name:')
        for dict_bp in best_props:
            for bp in dict_bp:
                print("\t\033[0;32m{}\x1b[0m".format(f"Dict: '{bp}' -> match: { round(dict_bp.get(bp).get('prop') * 100, 2) }% in line: { dict_bp.get(bp).get('skiprows') } sheet: '{ dict_bp.get(bp).get('sheet_name') }'"))
    else: print('\x1b[31;5m{}\x1b[0m'.format(f"Its not possible to open file, columns dont match > 70% in the sheets found"))

    return best_props


def check_colums_match(df, dicts, sheet_name=None):
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

            #Checagem para ver se todas as colunas do dict foram traduzidas
            if not any(search):
                error_cols.append(f"[{k}: {'|'.join(v)}], string searched in df: '{expression}'")

        #Prop das colunas encontradas
        df['_prop'] = df['_counter'] / len(dict_columns)

        #TODO check das colunas que foram renomeadas e que não tem nenhuma correspondencia com o dict

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

def unidecode_and_clean(x):
    x = unidecode.unidecode(str(x).lower().replace('\\n', ' ').replace('\n', ' ').strip())
    return " ".join(x.split())

def normalize_df_as_unique_col_names(df):
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

#Lê o arquivo csv e retorna traduzido para o read_auto
def xls_to_df(path, engine=None, sep=',', dicts=None, data_locations=None):
    print("===================================================")
    print("Opening file to Pandas DataFrame:")
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
    print("===================================================")
    return dfs

def normalize_cols_as_unique_col_names(df):
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

def columns(df, *args, **kwargs):
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