"""
Script with class to apply to sources
See: https://confluence.k8s2.grupocgd.com/display/AOAA/sources_global
"""
import logging
import os
import re
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from shyness.scripts.preparation.sources import Standardization
from shyness.scripts.processing.connectors import NetezzaConn
from ..preparation.validation import shyness_data_types
from difflib import SequenceMatcher
import multiprocessing as mp
from functools import partial

DATE_FORMAT_SOURCE = "%Y%m%d_%H%M%S"
# Global variable for duration tags
DATE_DURATION_TAGS = {" last ": '>='}
PARTITION_DATE_AVAILABLE = ['year', 'month', 'month_year', 'full_date', 'full_date_time']
logger = logging.getLogger(__name__)


class Filters:
    """
    Module for filters
    """

    @classmethod
    def by_query(cls, df: pd.DataFrame, condition_query: str) -> pd.DataFrame:
        """
        This function applies the filters to the given dataframe according to some query condition:
        :param df: Receives a DataFrame
        :param condition_query: Condition to apply in order to filter the DatFrame
        :return: DataFrame with the standardized strings
        """

        assert type(df) == pd.DataFrame, "The df parameter is not a DataFrame."
        assert type(condition_query) == str, "The condition_query parameter is not a string."
        df = df.query(condition_query)
        return df

    @classmethod
    def __by_query_date_preparation(cls, df: pd.DataFrame, condition_query: str,
                                    aux_col_name: str = 'aux_col') -> tuple:
        """
        This function prepares date information to run by_query_with_date function
        :param df: Receives a DataFrame
        :param condition_query: Condition to apply in order to filter the DatFrame
        partition_date available: year, month, month_year, full_date, full_date_time
        :param aux_col_name: name of the new column to apply the query
        :return: DataFrame with the standardized strings
        """
        # Analysing condition
        parts = condition_query.split(':')
        query = parts[1].split(" ")
        partition_date = parts[0]
        current_date = None

        # Verify if needs to be treated with duration tags
        verify_list = [x for x in DATE_DURATION_TAGS.keys() if x in condition_query]
        duration_tag = len(verify_list) > 0
        if duration_tag:
            current_date = datetime.now()
            logger.warning('date {}'.format(str(current_date)))
            logger.warning('date {}'.format(' '.join(query)))
            assert query[2].isdigit(), "The filter dates using duration is not valid"

        assert partition_date in PARTITION_DATE_AVAILABLE, \
            "The partition_date {} is not available. Check the partitions available: {}".format(
                partition_date, PARTITION_DATE_AVAILABLE)

        # Conversion to date col
        col = query[0]
        df.insert(df.shape[1], aux_col_name, pd.to_datetime(df[col], format=DATE_FORMAT_SOURCE, errors='coerce'))

        # Treating which date filter is needed
        if query[2] == 'nan':
            if query[1] == '==':
                query[1] = '!='
            elif query[1] == '!=':
                query[1] = '=='
            query[2] = aux_col_name
        elif partition_date == 'year':
            df.loc[:, aux_col_name] = df[aux_col_name].dt.year
            df.loc[:, aux_col_name] = df[aux_col_name].astype(float)
            if current_date is not None:
                current_date = current_date.year - int(query[2])
                query[1] = DATE_DURATION_TAGS[verify_list[0]]
                query[2] = str(current_date)
                logger.warning('date {}'.format(' '.join(query)))

        elif partition_date == 'month':
            df.loc[:, aux_col_name] = df[aux_col_name].dt.month
            df.loc[:, aux_col_name] = df[aux_col_name].astype(float)
            if current_date is not None:
                current_date = current_date.month - int(query[2])
                query[1] = DATE_DURATION_TAGS[verify_list[0]]
                query[2] = str(current_date)

        elif partition_date == 'month_year':
            df.loc[:, aux_col_name] = df[aux_col_name].dt.to_period('M')

        elif partition_date == 'full_date':
            if query[-1][0:4] == 'now+' and query[-1][4:].isdigit():
                current_date = datetime.now() + timedelta(int(query[-1][4:]))
                query[2] = current_date.strftime('%Y%m%d')
            elif query[-1][0:3] == 'now':
                current_date = datetime.now()
                query[2] = current_date.strftime('%Y%m%d')

        return df, query

    @classmethod
    def by_query_with_date(cls, df: pd.DataFrame, condition_query: str) -> pd.DataFrame:
        """
        This function applies the filters to the given dataframe according to some query condition:
        :param df: Receives a DataFrame
        :param condition_query: Condition to apply in order to filter the DatFrame
        partition_date available: year, month, month_year, full_date, full_date_time
        :return: DataFrame with the standardized strings
        """

        assert type(df) == pd.DataFrame, "The df parameter is not a DataFrame."
        assert type(condition_query) == str, "The condition_query parameter is not a string."

        # Create for cycle to prepare dates
        # possible patterns
        pattern = [' or ', ' and ']
        # Get position order of patterns
        regex = re.compile(r'\b(' + '|'.join(pattern) + r')\b')
        list_patterns = [p.group() for p in re.finditer(regex, condition_query)]

        # Create for cycle
        condition_query_aux = condition_query
        cols_to_drop = []
        for i in range(0, len(list_patterns) + 1):
            if i != len(list_patterns):
                condition_query_to_clean = condition_query_aux.split(list_patterns[i])[0]
            else:
                condition_query_to_clean = condition_query_aux
            # Call function to prepare dates and dataframe to execute the query
            df, query = \
                cls.__by_query_date_preparation(df, condition_query_to_clean, aux_col_name='aux_col' + str(i))
            if i != len(list_patterns):
                condition_query_aux = re.sub(
                    '{}{}'.format(condition_query_to_clean, list_patterns[i]), '', condition_query_aux)

            # Generate query to apply after clean
            query_to_apply = 'aux_col{} {}'.format(str(i), ' '.join(query[1:]))
            # Append the clean query to final_query
            if i == 0:
                final_query = query_to_apply
            else:
                final_query = '{}{}{}'.format(final_query, list_patterns[i - 1], query_to_apply)
            cols_to_drop.append('aux_col' + str(i))

        df = cls.by_query(df, final_query)
        df = df.drop(columns=cols_to_drop)
        return df


class Columns:
    """
    Class to change/select columns
    """

    @staticmethod
    def select(df: pd.DataFrame, **source_conf) -> pd.DataFrame:
        """
        This function gives the DataFrame with the choose columns:
        :param df: Receives a DataFrame
        :return: DataFrame with the choose columns
        """
        # Check df parameter
        assert type(df) == pd.DataFrame, "The df parameter is not a DataFrame."
        # Check cols parameter
        assert 'cols' in source_conf.keys(), "The cols is not given by the user"
        columns_to_retain = source_conf['cols'].replace('\n', '').split(",")

        assert type(columns_to_retain) == list, 'The columns_to_retain parameter must be a list'

        columns_to_retain = AuxiliaryFunctions.verify_and_choose_columns_by_tag(df, columns_to_retain)

        # Check if the new cols are added automatically to the list of columns to keep
        if 'add_automatically_new_cols' in source_conf:
            assert type(source_conf['add_automatically_new_cols']) == bool, \
                "The add_automatically_new_cols parameter must be a boolean"
            if source_conf['add_automatically_new_cols']:
                assert 'original_columns' in source_conf.keys(), \
                    "The original_columns is not given by the user"

                original_columns = source_conf['original_columns']

                assert type(original_columns) == list, "The original_columns must be a list"

                # Check if add_automatically_new_cols == True
                # then you already will add all the new columns
                # if they are in columns_to_retain remove it and maintain only the ones in original_columns
                columns_to_retain = [x for x in columns_to_retain if x in original_columns]

                # Select the columns added by the shad preparation functions to columns_top_retain
                new_columns_from_shad = [x for x in df.columns if x not in original_columns]
                columns_to_retain.extend(new_columns_from_shad)

        # Add a warning that some columns you gave in the columns_to_retain are not present in df7
        # and for that reason they will be not present in the final dataframe
        missing_cols = [col for col in columns_to_retain if col not in df.columns.tolist()]
        if len(missing_cols) != 0:
            logger.warning("The columns {} are missing from the dataframe. "
                           "For that reason they will be removed from columns_to_retain and therefore they will be not "
                           "present in the final dataframe.".format(', '.join(missing_cols)))
            columns_to_retain = [col for col in columns_to_retain if col not in missing_cols]

        df = df[columns_to_retain].drop_duplicates()

        return df

    @staticmethod
    def rename(df: pd.DataFrame, **source_conf) -> pd.DataFrame:
        """
        This function renames the DataFrame columns
        :param df: Receives a DataFrame
        """

        assert type(df) == pd.DataFrame, "The df parameter is not a DataFrame."
        assert 'new_names' in source_conf.keys(), "The new_columns_names is not given by the user"
        if type(source_conf['new_names']) == str:
            new_columns_names = source_conf['new_names'].split(",")
            assert type(new_columns_names) == list, 'The new_columns_names parameter must be a list'
            assert df.shape[1] == len(new_columns_names), \
                "The number of columns given {} is different from the number of dataframe columns {}" \
                .format(len(new_columns_names), df.shape[1])
            df.columns = new_columns_names
        elif type(source_conf['new_names']) == dict:
            new_columns_names = source_conf['new_names']
            # Verify if the columns have substitution_tags
            verify_cols = AuxiliaryFunctions.verify_and_choose_columns_by_tag(
                df, list(new_columns_names.keys()), return_dict=True)

            # Reformulate the cols_expected with the verify_cols result
            cols_expected = dict([
                (k1, k1.replace(k.replace(tag, ''), v))
                for k, v in new_columns_names.items()
                for k1 in verify_cols[k]['values']
                for tag in verify_cols[k]['tag']
            ])

            # Check if the columns are present in the dataframe if not warn the user
            missing_cols = [k for k in cols_expected.keys() if k not in df.columns.tolist()]
            if len(missing_cols) != 0:
                logger.warning("The columns: {} are not present in the dataframe. "
                               "They will be removed from the new_names".format(', '.join(missing_cols)))
                cols_expected = {k: v for k, v in cols_expected.items() if k not in missing_cols}
            df = df.rename(columns=cols_expected)
        else:
            raise NameError("The 'new_names' parameter must be a string or a dictionary.")

        return df


class Transformations:
    """
    Class with function to change sources content
    """

    @staticmethod
    def clients_to_drop(df: pd.DataFrame, **aux_sources) -> pd.DataFrame:
        """
        This function returns the DataFrame without duplicated nifs because of their multiple n_client
        Note the column names must be:
            n_client
            nif
            n_client_dropped: n_client dropped after fusion
            oe_dir_manager
            id_manager
            start_relationship_cgd
        :param df: Receives the base DataFrame
        :return: DataFrame with unique pairs (nif, n_client)
        """
        df_fusion = aux_sources['fusion_table']
        df_oe = aux_sources['info_client']
        assert type(df) == pd.DataFrame, "The df parameter is not a DataFrame."
        assert type(df_fusion) == pd.DataFrame, "The df_fusion parameter is not a DataFrame"
        assert type(df_oe) == pd.DataFrame, "The df_oe parameter is not a DataFrame"

        cols_df = ['n_cliente', 'n_contribuinte']
        cols_df_fusion = ['n_cliente']
        cols_df_oe = ['n_cliente', 'nm_reg_dirz_gerx', 'c_usid_gerx_cli', 'z_abra_cli']

        not_in_info_cols = [x for x in cols_df if x not in list(df.columns)]
        assert len(not_in_info_cols) == 0, 'The columns {0} are not present in df'.format(",".join(
            not_in_info_cols))

        not_in_info_cols = [x for x in cols_df_fusion if x not in list(df_fusion.columns)]
        assert len(not_in_info_cols) == 0, 'The columns {0} are not present in df_fusion'.format(",".join(
            not_in_info_cols))

        not_in_info_cols = [x for x in cols_df_oe if x not in list(df_oe.columns)]
        assert len(not_in_info_cols) == 0, 'The columns {0} are not present in df_oe'.format(",".join(
            not_in_info_cols))

        check_unique_nif = all(~df['n_contribuinte'].duplicated(keep=False))

        df_oe['z_abra_cli'] = pd.to_datetime(df_oe['z_abra_cli'], format='%Y-%m-%d',
                                             errors='coerce')

        if check_unique_nif:
            # logger.warning
            return df
        else:
            # Create indicator if the n_client was removed
            aux = df_fusion['n_cliente'].values.tolist()
            df['if_n_client_removed'] = df['n_cliente'] \
                .apply(lambda x: 1 if x in aux else 0)

            # Create indicator if the n_client doesn't have manager
            cond_client_without_manager = df_oe['c_usid_gerx_cli'] == 'C000000'
            client_without_manager = df_oe.loc[cond_client_without_manager, 'n_cliente'].values.tolist()

            df['if_n_client_without_manager'] = df['n_cliente'] \
                .apply(lambda x: 1 if x in client_without_manager else 0)

            # Create indicator if the n_client doesn't have OE dir
            cond_client_without_oe_dir = df_oe['nm_reg_dirz_gerx'] == ''
            client_without_oe_dir = df_oe.loc[cond_client_without_oe_dir, 'n_cliente'].values.tolist()

            df['if_n_client_without_oe_dir'] = df['n_cliente'] \
                .apply(lambda x: 1 if x in client_without_oe_dir else 0)

            # Create column indicating the number of n_contribuinte duplicated per n_contribuinte
            number_duplicated_nif = df.groupby('n_contribuinte', as_index=False) \
                .agg({'n_cliente': 'count'}) \
                .rename(columns={'n_cliente': 'number_duplicates'})
            df = df.merge(number_duplicated_nif,
                          on='n_contribuinte',
                          how='left'
                          )

            # Indicator of exclusion
            df['if_n_client_to_drop'] = 0
            df = df.sort_values('n_contribuinte')
            for index, row in df.iterrows():

                if (row['number_duplicates'] > 1) & (row['if_n_client_removed'] == 1):
                    df.loc[index, 'if_n_client_to_drop'] = 1
                    df.loc[df['n_contribuinte'] == row['n_contribuinte'],
                           'number_duplicates'] = row['number_duplicates'] - 1

                if (row['number_duplicates'] > 1) & (row['if_n_client_without_manager'] == 1):
                    df.loc[index, 'if_n_client_to_drop'] = 1
                    df.loc[df['n_contribuinte'] == row['n_contribuinte'],
                           'number_duplicates'] = row['number_duplicates'] - 1

                if (row['number_duplicates'] > 1) & (row['if_n_client_without_oe_dir'] == 1):
                    df.loc[index, 'if_n_client_to_drop'] = 1
                    df.loc[df['n_contribuinte'] == row['n_contribuinte'],
                           'number_duplicates'] = row['number_duplicates'] - 1

                if row['number_duplicates'] > 1:
                    date1 = df_oe.loc[df_oe['n_cliente'] == df.loc[index, 'n_cliente'], 'z_abra_cli'].values
                    date2 = df_oe.loc[df_oe['n_cliente'] == df.loc[index + 1, 'n_cliente'],
                                      'z_abra_cli'].values
                    if date1 > date2:
                        df.loc[index + 1, 'if_n_client_to_drop'] = 1
                    elif date2 > date1:
                        df.loc[index, 'if_n_client_to_drop'] = 1
                    df.loc[df['n_contribuinte'] == row['n_contribuinte'],
                           'number_duplicates'] = row['number_duplicates'] - 1

                if np.isnan(row['n_contribuinte']):
                    df.loc[index, 'if_n_client_to_drop'] = 1
            df = df.reset_index()
            df = df.drop(columns=['index',
                                  'if_n_client_removed',
                                  'if_n_client_without_manager',
                                  'if_n_client_without_oe_dir'])

        return df

    @staticmethod
    def contains_content(df, **configurations) -> pd.DataFrame:
        """
        Function that selects the content of auxiliary sources on the base source
        :param df: dataframe of the source base
        :param configurations: dictionary with configuration settings
        :return:
        """
        try:
            # Check configurations content
            allowed_keys = ['aux_source', 'col_base', 'col_aux']
            verify_keys = [x for x in allowed_keys if x not in configurations.keys()]
            assert len(verify_keys) == 0, "The parameters: {} are missing".format(", ".join(verify_keys))

            aux_source_tag = configurations['aux_source']
            col_base = configurations['col_base']
            col_aux = configurations['col_aux']

            assert aux_source_tag in configurations.keys(), "The auxiliary source {} is missing".format_map(
                aux_source_tag)

            aux_source = configurations[aux_source_tag]

            assert type(df) == pd.DataFrame, "The df parameter must be a DataFrame"
            assert type(aux_source) == pd.DataFrame, "The aux_source parameter must be a DataFrame"
            assert type(col_base) == str, "The col_base parameter must be str"
            assert type(col_aux) == str, "The col_aux parameter must be str"
            assert col_base in df.columns.tolist(), 'The {0} are not present in df'.format(col_base)
            assert col_aux in aux_source.columns.tolist(), 'The {0} are not present in aux_source'.format(col_aux)

            # Check content and slice dataframe based on it
            content = aux_source[col_aux].unique().tolist()

            if 'with_indicator' in configurations.keys():

                assert 'indicator_col_name' in configurations['with_indicator'].keys()
                assert 'positive_value' in configurations['with_indicator'].keys()
                assert 'default_value' in configurations['with_indicator'].keys()

                indicator_col_name = configurations['with_indicator']['indicator_col_name']
                positive_value = configurations['with_indicator']['positive_value']
                default_value = configurations['with_indicator']['default_value']
                df.insert(df.shape[1], indicator_col_name, default_value)
                df.loc[df[col_base].isin(content), indicator_col_name] = positive_value

            else:
                df = df[df[col_base].isin(content)]

            return df

        except Exception as e:
            logger.error(e)
            raise e

    @staticmethod
    def match_names(df, **configurations) -> pd.DataFrame:
        """
        Function to match names between dataframes and add an id columns
        :param df: dataframe of the source base
        :param configurations: dictionary with configuration settings
        :return:
        """

        # Check configurations content
        allowed_keys = ['aux_source', 'col_base', 'col_aux', 'col_aux_add']
        verify_keys = [x for x in allowed_keys if x not in configurations.keys()]
        assert len(verify_keys) == 0, "The parameters: {} are missing".format(", ".join(verify_keys))

        aux_source_tag = configurations['aux_source']
        col_base = configurations['col_base']
        col_aux = configurations['col_aux']
        col_aux_add = configurations['col_aux_add']

        assert aux_source_tag in configurations.keys(), "The auxiliary source {} is missing".format_map(aux_source_tag)

        aux_source = configurations[aux_source_tag]

        assert type(df) == pd.DataFrame, "The df parameter must be a DataFrame"
        assert type(aux_source) == pd.DataFrame, "The aux_source parameter must be a DataFrame"
        assert type(col_base) == str, "The col_base parameter must be str"
        assert type(col_aux) == str, "The col_aux parameter must be str"
        assert type(col_aux_add) == str, "The col_aux_add parameter must be str"
        assert col_base in df.columns.tolist(), 'The {0} are not present in df'.format(col_base)
        assert col_aux in aux_source.columns, 'The {0} are not present in aux_source'.format(col_aux)

        # Filter names to match
        names_to_match = aux_source[[col_aux]].drop_duplicates()[col_aux].values.tolist()
        names_base = df[[col_base]].drop_duplicates()[col_base].values.tolist()

        # Alert for names that not match
        for name in names_base:
            if name not in names_to_match:
                logger.critical('NO MATCH >> {} not in the suppliers'.format(name))

        # Merge between sources to add an id
        aux_source = aux_source[[col_aux, col_aux_add]]
        aux_source.columns = [col_base, col_aux_add]
        df = pd.merge(df, aux_source, on=col_base, how='inner')

        return df

    @staticmethod
    def add_nif(df: pd.DataFrame, **configurations) -> pd.DataFrame:
        """
        This function returns the DataFrame with nif
        :param df: Receives the DataFrame to add column with nif
        :return: DataFrame with nif
        """
        # Check configurations content
        allowed_keys = ['aux_source', 'col_base', 'col_aux', 'col_aux_add']
        verify_keys = [x for x in allowed_keys if x not in configurations.keys()]
        assert len(verify_keys) == 0, "The parameters: {} are missing".format(", ".join(verify_keys))

        aux_source_tag = configurations['aux_source']
        col_base = configurations['col_base']
        col_aux = configurations['col_aux']
        col_aux_add = configurations['col_aux_add']

        assert aux_source_tag in configurations.keys(), "The auxiliary source {} is missing".format_map(aux_source_tag)

        df_with_nif = configurations[aux_source_tag]

        assert type(df) == pd.DataFrame, "The df_with_nif parameter is not a DataFrame."
        assert type(df_with_nif) == pd.DataFrame, "The df_with_nif parameter is not a DataFrame."
        assert type(col_base) == str, "The col_base parameter must be str"
        assert type(col_aux) == str, "The col_aux parameter must be str"
        assert type(col_aux_add) == str, "The col_aux_add parameter must be str"
        assert col_base in df.columns.tolist(), 'The {0} are not present in df'.format(col_base)
        assert col_aux in df_with_nif.columns, 'The {0} are not present in aux_source'.format(col_aux)
        assert col_aux_add in df_with_nif.columns, 'The {0} are not present in aux_source'.format(col_aux)

        df_with_nif.loc[:] = Standardization.nifs(df_with_nif, [col_aux_add])

        # Merge between sources to add an id
        df_with_nif = df_with_nif[[col_aux, col_aux_add]]
        df_with_nif.columns = [col_base, col_aux_add]
        df = df.merge(
            df_with_nif,
            on=col_base,
            how='left'
        )
        return df

    @staticmethod
    def add_info(df: pd.DataFrame, **configurations) -> pd.DataFrame:
        """
        Adds new columns from different sources to the base dataframe
        :param df: base dataframe
        :param configurations: dictionary with all input variables
        :return: changed dataframe with new columns
        """
        try:
            logger.info('Started add_info')
            # Verify input
            assert type(df) == pd.DataFrame, "The df parameter is not a DataFrame."

            # Check configurations content: if the necessary keys are present
            allowed_keys = ['aux_source', 'col_base', 'col_aux', 'cols_aux_to_add']
            verify_keys = [x for x in allowed_keys if x not in configurations.keys()]
            assert len(verify_keys) == 0, "The parameters: {} are missing".format(", ".join(verify_keys))

            aux_source_tag = configurations['aux_source']
            col_base = configurations['col_base']
            col_aux = configurations['col_aux']
            cols_aux_to_add = configurations['cols_aux_to_add']

            # Check configurations content: if the auxiliary source was loaded and is present in configurations
            assert aux_source_tag in configurations.keys(), \
                "The auxiliary source {} is missing".format(aux_source_tag)
            aux_source = configurations[aux_source_tag]

            # Check configurations content: check type of the content
            assert type(aux_source) == pd.DataFrame, "The aux_source parameter must be a DataFrame"
            assert type(col_base) == str, "The col_base parameter must be str. If more than one separate them by comma"
            assert type(col_aux) == str, "The col_aux parameter must be str. If more than one separate them by comma"
            assert type(cols_aux_to_add) == str, \
                "The cols_aux_to_add parameter must be str. If more than one separate them by comma"
            cols_aux_to_add = cols_aux_to_add.split(',')
            # if there are more than one column to be merged they will be separated by comma
            col_base = configurations['col_base'].split(',')
            col_aux = configurations['col_aux'].split(',')

            # Check that they are the same length
            assert len(col_base) == len(col_aux), "The length of columns in col_base and col_aux must be the same"

            # Check configurations content: Check if the col_base is present in df
            missing_cols = [col for col in col_base if col not in df.columns.tolist()]
            assert len(missing_cols) == 0, 'The columns {} are not present in df'.format(', '.join(col_base))

            # Verify if the columns have substitution tags
            cols_aux_to_add = AuxiliaryFunctions.verify_and_choose_columns_by_tag(aux_source, cols_aux_to_add)

            # Check configurations content: Check if the col_aux and cols_aux_to_add are present in aux_source
            cols_aux_to_check = cols_aux_to_add
            cols_aux_to_check.extend(col_aux)
            missing_cols = [col for col in cols_aux_to_check if col not in aux_source.columns.tolist()]
            assert len(missing_cols) == 0, \
                'Columns {} are not in the {}'.format(', '.join(missing_cols), aux_source_tag)

            logger.info('Input Variables: OK!')

            # If substitute_na_values in the keys then substitute the na values in col_base and
            # col_aux by the value given
            if 'substitute_na_values' in configurations.keys():
                substitute_na_values = configurations['substitute_na_values']
                if len(col_base) > 1:
                    # Check if col_base has more than one column then the substitute_na_values
                    # is a list with the same length
                    assert type(substitute_na_values) == list, \
                        "The col_base has more than one column, therefore the substitute_na_values parameter " \
                        "must be a list with the value to substitute in each column respectively"
                    assert len(col_base) == len(substitute_na_values), \
                        "The substitute_na_values must have the same length as col_base"

                    # Substitute na value
                    for i in range(0, len(col_base)):
                        df.loc[:, col_base[i]] = df[col_base[i]].fillna(substitute_na_values[i])
                        aux_source.loc[:, col_aux[i]] = aux_source[col_aux[i]].fillna(substitute_na_values[i])
                else:
                    df.loc[:, col_base] = df[col_base].fillna(substitute_na_values)
                    aux_source.loc[:, col_aux] = aux_source[col_aux].fillna(substitute_na_values)

            # if col_type in the keys then change the type of the merge columns in the respective dataframes
            if 'col_type' in configurations.keys():
                assert type(configurations['col_type']) == str, "The col_type parameter must be a string"
                df.loc[:, col_base] = df.loc[:, col_base].astype(configurations['col_type'])
                aux_source.loc[:, col_aux] = aux_source.loc[:, col_aux].astype(configurations['col_type'])

            aux_source = aux_source[cols_aux_to_add]

            # if post_name in keys then, beyond the addition of aux_source_tag to the beginning of aux cols,
            # add the given post_name name to the end of the auxiliary sources name
            # other wise only add to the beginning of the name the aux_source_tag
            if 'post_name' in configurations.keys():
                assert type(configurations['post_name'] == str), "The post_name parameter must be a string"
                cols_aux_to_add = ['{}_{}_{}'.format(aux_source_tag, x, configurations['post_name'])
                                   for x in cols_aux_to_add]
            else:
                cols_aux_to_add = ['{}_{}'.format(aux_source_tag, x)
                                   for x in cols_aux_to_add]

            # Change the name of the col_aux to the col_base
            # the len(col_aux) is to accommodate the case: col_base/col_aux are more than one column
            cols_aux_to_add[-len(col_aux):] = col_base

            # if validation_shyness_data_types in configurations keys then pass the auxiliary source
            # by the function to change the columns by the indications in shyness yaml
            if 'validation_shyness_data_types' in configurations.keys():
                assert type(configurations['validation_shyness_data_types']) == str, \
                    "The validation_shyness_data_types must be a boolean"
                validation_conf = configurations['validation_shyness_data_types'].split(':')
                aux_source = shyness_data_types(aux_source, product=validation_conf[0], src_tag=validation_conf[1])

            logger.info("Start Indicador")
            aux_source.columns = cols_aux_to_add

            # Remove duplicated rows
            aux_source = aux_source.drop_duplicates()

            # If how in configurations keys then change how to the value given by the user
            # otherwise the default is 'left'
            if 'how' in configurations.keys() and configurations['how'] is not None:
                how = configurations['how']
                assert type(how) == str, "The 'how' parameter in configurations must be a string"
                valid_how_values = ['left', 'right', 'outer', 'inner', 'cross']
                assert how in valid_how_values, \
                    "The 'how' parameter value {} is invalid. Check list: {}" \
                    .format(how, ', '.join(valid_how_values))
            else:
                how = 'left'

            # If indicator in configurations keys ans have the value true
            # the indicator param in pd.merge is set to true
            # That is add an indicator that is '1' if the value is in both dataframes and 0 otherwise
            if 'indicator' in configurations.keys() and configurations['indicator']:
                df = pd.merge(df, aux_source, on=col_base, how=how, indicator=True)

                df.loc[:, '_merge'] = df['_merge'].apply(
                    lambda x: 1 if x == 'both'
                    else 0
                )

                df = df.rename(columns={'_merge': 'if_in_{}'.format(aux_source_tag)})
            else:
                df = pd.merge(df, aux_source, on=col_base, how=how)

            # Change the columns names => if the column have a date transfer to the end of the col name
            df = AuxiliaryFunctions.add_date_end_colname(df)
            logger.info('Added cols: {} on {}'.format(','.join(cols_aux_to_add), aux_source_tag))

            # Check if the resulted dataframe have duplicated col_names
            # if it is true devolve a warning advising the user to that
            if_duplicated_cols = [x for x in list(df.columns) if x.endswith('_x') or x.endswith('_y')]

            if len(if_duplicated_cols) > 0:
                logger.warning("Exists duplicated column names, consider giving to the function the parameters: "
                               "'post_name'")

            return df

        except Exception as e:
            logger.error(e)
            raise e

    @staticmethod
    def prepare_contacts_info(df: pd.DataFrame, **configurations) -> pd.DataFrame:
        """
        Prepare Contacts Information
        :param df: base dataframe
        :param configurations: dictionary with all input variables
        :return: changed dataframe with new contact columns
        """
        try:
            logger.info('Started prepare_contacts_info')
            # Check input parameters
            assert type(df) == pd.DataFrame, "The df parameter must be a DataFrame."

            # Check configurations content
            allowed_keys = ['col_id', 'col_contacts', 'new_cols_contacts_tag']
            verify_keys = [x for x in allowed_keys if x not in configurations.keys()]
            assert len(verify_keys) == 0, "The parameters: {} are missing".format(", ".join(verify_keys))

            col_id = configurations['col_id']
            col_contacts = configurations['col_contacts']
            new_cols_contacts_tag = configurations['new_cols_contacts_tag']

            # Count maximum number of contacts
            df_not_empty = df[df[col_contacts] != '']
            max_contacts = df_not_empty[[col_id, col_contacts]].drop_duplicates().groupby([col_id]).count().max()

            # For each entity fill the contacts columns
            df_contacts = df[[col_id]].drop_duplicates()
            if max_contacts[0] > 0:
                # Create max_contacts columns
                for col_i in range(1, max_contacts[0] + 1):
                    df_contacts.insert(df_contacts.shape[1],
                                       '{}_{}'.format(new_cols_contacts_tag, col_i),
                                       '')

                # for each entity fill the contact columns
                for index, row in df_contacts.iterrows():
                    entity = row[col_id]
                    all_contacts = df[(df[col_id] == entity) & (df[col_contacts] != '')][[col_contacts]] \
                        .drop_duplicates()[col_contacts].values.tolist()
                    all_contacts = [c for c in all_contacts if len(str(c)) > 0]
                    for i in range(len(all_contacts)):
                        df_contacts.loc[index, '{}_{}'.format(new_cols_contacts_tag, i + 1)] = all_contacts[i]

                df = df.drop(columns=[col_contacts])
                df = pd.merge(
                    df,
                    df_contacts,
                    on=col_id,
                    how='left'
                ).drop_duplicates()

            logger.info('Prepared contacts information for: {}'.format(col_contacts))

            return df

        except Exception as e:
            logger.error(e)
            raise e

    @staticmethod
    def merge_into_one_column(df: pd.DataFrame, **configurations) -> pd.DataFrame:
        """
        Function that merge values of two columns in one (default, nan or NAT)
        :param df: data frame
        :type df: pandas.DataFrame
        :param configurations: dictionary with configurations needed
        :type configurations: dict
        :return: changed DataFrame
        :rtype: pandas.DataFrame
        """
        try:
            logger.info("Start merge_into_one_columns")
            # verify input variables
            assert type(df) == pd.DataFrame, "The df must be a DataFrame"
            assert type(configurations) == dict, "The configurations must be a dictionary"

            df_columns = list(df.columns)

            # verify the settings for the configuration
            assert 'target_col' in configurations.keys(), "Must be a target_col key on configurations"
            assert type(configurations['target_col']) == str, "The target_col must be a string"
            target_col = configurations['target_col']

            assert 'first_col' in configurations.keys(), "Must be a first_col key on configurations"
            assert type(configurations['first_col']) == str, "The first_col must be a string"
            assert configurations['first_col'] in df_columns, "The {} does not exist in df".format(
                configurations['first_col'])
            first_col = configurations['first_col']

            assert 'second_col' in configurations.keys(), "Must be a second_col on configurations"
            assert type(configurations['second_col']) == str, "The second_col must be a string"
            assert configurations['second_col'] in df_columns, "The {} does not exist in df".format(
                configurations['second_col'])
            second_col = configurations['second_col']
            if 'col_type' in configurations.keys():
                df = df.astype({first_col: configurations['col_type'], second_col: configurations['col_type']})

            logger.info("Configurations on merge_into_one_columns: OK")
            if target_col not in df_columns:
                df.insert(df.shape[1], target_col, "")

            if 'values_to_replace' in configurations.keys():
                values_to_replace = configurations['values_to_replace']
                assert type(values_to_replace) == list, "The values to replace is not a list"
                # Check types:
                col_type = df[first_col].dtypes
                types_in_list = list(set([type(x) for x in values_to_replace]))
                if len(types_in_list) > 1:
                    logger.warning('The values_to_replace list have more than one type {}'
                                   .format(', '.join(types_in_list)))
                elif types_in_list[0] != col_type:
                    logger.warning('The type in list {} is different from the column type {}.'
                                   .format(types_in_list[0], col_type))

                df.loc[:, target_col] = df.apply(
                    lambda x: x[first_col] if x[first_col] not in values_to_replace
                    else x[second_col],
                    axis=1
                )

            else:
                df.loc[:, target_col] = df.apply(
                    lambda x: x[first_col]
                    if str(x[first_col]) != 'NaN' and str(x[first_col]) != "NaT"
                    and str(x[first_col]) != "nan" and len(str(x[first_col])) > 0
                    else x[second_col],
                    axis=1
                )
            logger.info("merge_into_one_columns: DONE")

            return df

        except Exception as e:
            logger.error(e)

    @staticmethod
    def add_n_client(df: pd.DataFrame, **configurations) -> pd.DataFrame:
        """
        Function to add n_cliente to the base DataFrame given the nif column
        :param df: data frame
        :type df: pandas.DataFrame
        :param configurations: dictionary with configurations needed
        :type configurations: dict
        :return: changed DataFrame
        :rtype: pandas.DataFrame
        """
        try:
            logger.info("Start add_n_client transformation")
            # verify input variables
            assert type(df) == pd.DataFrame, "The df must be a DataFrame"
            assert type(configurations) == dict, "The configurations must be a dictionary"

            assert 'nif_col' in configurations.keys(), "Must be a nif_col key on configurations"
            assert type(configurations['nif_col']) == str, "The id_cols must be a string"

            nif_col = configurations['nif_col']
            assert nif_col in df.columns, "The column {} is not present in df".format(nif_col)

            assert 'nif_col_type' in configurations.keys(), "Must be a nif_col_type key on configurations"
            assert type(configurations['nif_col_type']) == str, "The nif_col_type must be a string"
            nif_col_type = configurations['nif_col_type']

            # TODO: add user and password to the configurations
            missing_conf = [k for k in ['user', 'password', 'env'] if k not in configurations.keys()]
            assert len(missing_conf) == 0, "The keys {} are missing from configurations".format(', '.join(missing_conf))

            user = configurations['user']
            password = configurations['password']
            env = configurations['env']
            assert type(user) == str, "The user must be a string"
            assert type(password) == str, "The password must be a string"
            assert type(env) == str, "The env must be a string"

            assert user != '', "The user is invalid"
            assert password != '', "The password is invalid"
            assert env in ['dev', 'prod'], "The env is invalid"

            # Create temp files
            parent_folder = "."
            possible_path = os.path.join(parent_folder, 'temp_aux_sources')
            if not os.path.exists(possible_path):
                os.mkdir(possible_path)

            # Selected nif column
            id_cols_source = df[nif_col].drop_duplicates()

            # Save as local file
            id_cols_source.to_csv(os.path.join(possible_path, "nif_col.csv"),
                                  index=False, encoding='UTF-8-SIG', sep=',')

            query_params = {
                'insert_into':
                    {'temp_file': {
                        'temp_file': os.path.abspath(os.path.join(possible_path, "nif_col.csv"))
                    }}
            }

            # Run query to get n_cliente
            conn = NetezzaConn(user, password, env)
            conn.create()
            aux_source = conn.select_query('get_n_client.sql',
                                           'master',
                                           [nif_col, 'n_cliente'],
                                           True,
                                           **query_params)

            aux_source.loc[:, nif_col] = aux_source[nif_col].astype(nif_col_type).str.strip()
            df.loc[:, nif_col] = df[nif_col].astype(nif_col_type)

            df = pd.merge(
                df,
                aux_source,
                on=nif_col,
                how='left'
            )
            df.loc[:, 'n_cliente'] = df['n_cliente'].astype(nif_col_type).str.strip()

            logger.info("add_n_client: DONE")
            return df
        except Exception as e:
            logger.error(e)

    @staticmethod
    def group_info(df: pd.DataFrame, **configurations) -> pd.DataFrame:
        """
        Function to group information from the base DataFrame with the given columns
        :param df: data frame
        :type df: pandas.DataFrame
        :param configurations: dictionary with configurations needed
        :type configurations: dict
        :return: changed DataFrame
        :rtype: pandas.DataFrame
        """
        try:
            logger.info("Start group_info transformation")

            # verify input variables
            assert type(df) == pd.DataFrame, "The df parameter must be a DataFrame"
            assert type(configurations) == dict, "The configurations must be a dictionary"

            # Verify the columns to group by
            assert 'cols_to_group_by' in configurations.keys(), "Must be a cols_to_group_by key on configurations"
            assert type(configurations['cols_to_group_by']) == str, "The cols_to_group_by must be a string"
            cols_to_group = configurations['cols_to_group_by'].split(',')

            # Check if cols_to_group_by have substitution_tags
            cols_to_group = AuxiliaryFunctions.verify_and_choose_columns_by_tag(df, cols_to_group)

            # Check if cols_to_group are present in the dataframe
            missing_cols = [x for x in cols_to_group if x not in df.columns]
            assert len(missing_cols) == 0, \
                'The columns {} are missing from the DataFrame'.format(', '.join(missing_cols))

            # Verify aggregation parameters
            assert 'agg_params' in configurations.keys(), "Must be a agg_params key on configurations"

            # Check columns in aggregation parameters
            agg_params = configurations['agg_params']
            assert type(agg_params) == dict, "The agg_params must be a dictionary"
            # Check if agg_params have ','
            # Check if agg_params have columns with substitution_tags
            # And in the end reformulate the dictionary
            agg_params = {k1: v
                          for k, v in agg_params.items()
                          for k1 in AuxiliaryFunctions.verify_and_choose_columns_by_tag(df, k.split(','))}

            # Verify if all the columns in agg_params are in the dataframe
            missing_cols = [k for k in agg_params.keys() if k not in df.columns]
            assert len(missing_cols) == 0, \
                'The columns {} are missing from the DataFrame'.format(', '.join(missing_cols))

            # Check the aggregation functions
            allowed_functions = ['min', 'max', 'mean', 'sum', 'count', 'nunique', 'median', 'std']
            incorrect_functions = [x for values in agg_params.values()
                                   for x in values if x not in allowed_functions]

            assert len(incorrect_functions) == 0, \
                "The functions {} are not allowed. Check the list: {}" \
                .format(', '.join(incorrect_functions), ', '.join(allowed_functions))

            df_group = df \
                .groupby(cols_to_group) \
                .agg(agg_params).reset_index()

            if 'metric_name_position' in configurations.keys() and configurations['metric_name_position'] is not None:
                pos = configurations['metric_name_position']
                assert type(pos) == str, 'The metric_name_position parameter must be a string'
                assert pos in ['start', 'end'], 'The metric_name_position parameter must be start or end'
            else:
                pos = 'end'

            if pos == 'end':
                df_group.columns = ['{}_{}'.format(x[0], x[1]) if x[0] not in cols_to_group else x[0]
                                    for x in df_group.columns.ravel()]
            elif pos == 'start':
                df_group.columns = ['{}_{}'.format(x[1], x[0]) if x[0] not in cols_to_group else x[0]
                                    for x in df_group.columns.ravel()]

            # Check other_cols parameter
            if 'other_cols' in configurations.keys():
                assert type(configurations['other_cols']) == str, "The other_cols must be a string"
                other_cols = configurations['other_cols'].split(',')
                missing_cols = [x for x in other_cols if x not in df.columns]
                assert len(missing_cols) == 0, \
                    'The columns {} are missing from the DataFrame'.format(', '.join(missing_cols))
                # add other_cols to the bse DataFrame
                if 'merge_on_non_unique_cols' in configurations.keys() and configurations['merge_on_non_unique_cols']:
                    # rename the columns that will be considered for the right_on
                    # (otherwise, the merge will not work)
                    agg_params_keys = list(agg_params.keys())
                    cols_to_rename = [col for col in df.columns if
                                      (col not in (agg_params_keys + other_cols)) and (col in cols_to_group)]
                    new_names = [col + '_df' for col in cols_to_rename]
                    df.rename(columns=dict(zip(cols_to_rename, new_names)), errors="raise", inplace=True)
                    df_group = pd.merge(
                        df_group,
                        df[new_names + agg_params_keys + other_cols].drop_duplicates(),
                        how='left',
                        left_on=list(df_group.columns),
                        right_on=(new_names + agg_params_keys)
                    )
                else:
                    other_cols.extend(cols_to_group)
                    df_group = pd.merge(
                        df_group,
                        df[other_cols].drop_duplicates(),
                        how='left',
                        on=cols_to_group
                    )
            logger.info('Final Columns {}'.format(', '.join(df_group.columns.tolist())))
            return df_group

        except Exception as e:
            logger.error(e)
            raise e

    @classmethod
    def fill_na_values(cls, df: pd.DataFrame, **configurations) -> pd.DataFrame:
        """
        Change Na ans string 'nan' values in columns by a given value
        :param df:
        :param configurations:
        :return:
        """
        try:
            logger.info("Start fill_na_values")
            # Check input types
            assert type(df) == pd.DataFrame, "The dataframe parameter must be a DataFrame"
            assert type(configurations) == dict, "The configurations parameter must be a Dictionary"

            # Check needed keys
            needed_keys = ['cols', 'value']
            missing_keys = [k for k in needed_keys if k not in configurations.keys()]
            assert len(missing_keys) == 0, \
                "The keys {} are missing from configurations".format(missing_keys)

            cols = configurations['cols']
            value = configurations['value']

            # Check cols, values types
            assert type(cols) == str, \
                "The cols key in configurations must be a string, a string of columns separated by comma"
            cols = cols.split(',')
            assert type(value) != list, \
                "The value key must be a string/int/float/etc, in na values in " \
                "all columns given will be changed by the value {}".format(value)

            # Verify if the columns given have substitution tags
            all_cols = AuxiliaryFunctions.verify_and_choose_columns_by_tag(df, cols)

            # Check if the columns are present in the dataframe
            missing_cols = [col for col in all_cols if col not in df.columns.tolist()]
            assert len(missing_cols) == 0, \
                "The columns {} are missing from the dataframe".format(', '.join(missing_cols))

            # Change na and 'nan' values by value
            for col in all_cols:
                df.loc[((df[col].isna()) | (df[col] == 'nan')), col] = value

            return df

        except Exception as e:
            logger.error(e)

    @classmethod
    def change_col_type(cls, df: pd.DataFrame, **configurations) -> pd.DataFrame:
        """
        Change type of a column except for dates
        for dates exists change_date_col
        :param df:
        :param configurations:
        :return:
        """
        try:
            # Check input types
            assert type(df) == pd.DataFrame, "The df parameter must be a DataFrame"
            assert type(configurations) == dict, "The configurations must be a dictionary"

            # Check cols in configurations
            assert 'cols' in configurations.keys(), "The cols key is missing from configurations"

            if configurations['cols'] is not None:
                assert type(configurations['cols']) == str, \
                    "The cols key in configurations must be a string with the columns separated by comma"
                cols = configurations['cols'].split(',')

                # Check if the column names don't have substitution tags
                cols = AuxiliaryFunctions.verify_and_choose_columns_by_tag(df, cols)

                # Check if cols are present in df
                missing_cols = [col for col in cols if col not in df.columns.tolist()]
                assert len(missing_cols) == 0, \
                    "The cols {} are missing from the dataframe".format(', '.join(missing_cols))
            else:
                cols = df.columns.tolist()

            # Validate datatype
            assert 'datatype' in configurations.keys(), "The datatype is missing from configurations"
            datatype = configurations['datatype']
            assert type(datatype) == str, "The datatype keys must be a string"
            available_data_types = ['int', 'int64', 'float', 'str', 'datetime']
            assert datatype in available_data_types, \
                "The datatype given is not valid. Check the list: {}. "

            # Change datatype
            if datatype != 'datetime':
                df.loc[:, cols] = df.loc[:, cols].astype(datatype)
            else:
                # Verify if the format is in the configurations
                assert 'datetime_format' in configurations.keys(), \
                    "The datetime_format key is missing from configurations"
                # Verify datetime_format is a string
                datetime_format = configurations['datetime_format']
                assert type(datetime_format) == str, "The datetime_format is not a string"

                for col in cols:
                    df.loc[:, col] = pd.to_datetime(df[col], format=datetime_format, errors='coerce')

            assert type(df) == pd.DataFrame, \
                "The function couldn't convert the cols {} to the data type pretended {}" \
                .format(', '.join(cols), datatype)

            return df

        except Exception as e:
            logger.error(e)

    @staticmethod
    def change_date_col(df: pd.DataFrame, **configurations) -> pd.DataFrame:
        """
        Function to change date_col to a given format
        :param df: data frame
        :type df: pandas.DataFrame
        :param configurations: dictionary with configurations needed
        :type configurations: dict
        :return: changed DataFrame
        :rtype: pandas.DataFrame
        """
        try:
            logger.info("Start change_date_col transformation")

            # verify input variables
            assert type(df) == pd.DataFrame, "The df parameter must be a DataFrame"
            assert type(configurations) == dict, "The configurations must be a dictionary"

            # verify mandatory keys
            missing_keys = [x for x in ['date_col', 'new_format'] if x not in configurations.keys()]
            assert len(missing_keys) == 0, \
                "The keys {} are missing from configurations".format(', '.join(missing_keys))

            # verify keys types
            date_col = configurations['date_col']
            new_format = configurations['new_format']
            assert type(date_col) == str, " The date_col parameter must be a string"
            assert type(new_format) == str, " The date_col parameter must be a string"
            # TODO: Verify if new_format date is valid

            # Change column to the new format
            if 'name_col' not in configurations.keys():
                name_col = date_col
            else:
                name_col = configurations['name_col']
                assert type(name_col) == str, "The name_col parameter must be a string"

            df.loc[:, name_col] = df[date_col].dt.strftime(date_format=new_format)

            if 'to_datetime' in configurations.keys():
                assert type(configurations['to_datetime']) == bool, \
                    "The to_datetime parameter must be a boolean"
                to_datetime = configurations['to_datetime']
            else:
                to_datetime = True

            if to_datetime:
                df.loc[:, name_col] = pd.to_datetime(df[name_col], format=new_format, errors='coerce')

            return df
        except Exception as e:
            logger.error(e)

    @staticmethod
    def transpose_column(df: pd.DataFrame, **configurations) -> pd.DataFrame:
        """
        Transpose a column
        :param df: base dataframe
        :param configurations:
        :return:
        """
        try:
            logger.info('Started transpose_column')
            # Verify input types
            assert type(df) == pd.DataFrame, "The df parameter must be a dataframe."
            assert type(configurations) == dict, "The configurations parameter must be a dictionary"

            # Verify input configuration keys
            needed_keys = ['col_to_transpose', 'id_col']
            missing_keys = [k for k in needed_keys if k not in configurations.keys()]
            assert len(missing_keys) == 0, \
                "The keys {} are missing from configurations".format(', '.join(missing_keys))

            col_to_transpose = configurations['col_to_transpose']
            id_col = configurations['id_col']
            # Verify types
            assert type(col_to_transpose) == str, \
                "The col_to_transpose must be a string with the name of the column to transpose"
            assert type(id_col) == str, \
                "The id_col must be a string with the name of the column containing the id"
            id_col = id_col.split(',')

            # Verify if the col_to_transpose have a substitution_tag:
            # col_to_transpose will be a list
            col_to_transpose = [col_to_transpose]
            col_to_transpose = AuxiliaryFunctions.verify_and_choose_columns_by_tag(df, col_to_transpose)

            # Verify if the id_cols and transpose_cols are present in dataframe
            missing_cols = [col for col in col_to_transpose if col not in df.columns.tolist()]
            assert len(missing_cols) == 0, \
                "The cols {} are missing from the dataframe".format(', '.join(missing_cols))
            missing_cols = [col for col in id_col if col not in df.columns.tolist()]
            assert len(missing_cols) == 0, \
                "The col(s) {} are missing from the dataframe".format(', '.join(missing_cols))

            # Check if the user give the categories_to_col
            # If not: all the values presents in col_to_transpose will be converted in column
            if 'categories_to_col' in configurations.keys() and configurations['categories_to_col'] is not None:
                # Verify if values_to_col is a list
                assert type(
                    configurations['categories_to_col']) == list, "The categories_to_col parameter must be a list"
                original_categories_to_col = configurations['categories_to_col']
            else:
                original_categories_to_col = []

            # Check if the user gives the col_with_values
            # if not it will be attributed an empty string
            if 'col_with_values' in configurations.keys() and configurations['col_with_values'] is not None:
                col_with_values = configurations['col_with_values']
                # CHeck type
                assert type(col_with_values) == str, "The col_with_values parameter must be a string"
                # Check if col_with_values is present in the dataframe
                assert col_with_values in df.columns.tolist(), \
                    "The columns {} are missing from the dataframe".format(col_with_values)
            else:
                col_with_values = ''

            # Check if the user give fillna parameter if not the na values will be nan
            # unless they didn't give col_with_values in that case fillna will be 0
            if 'fillna' in configurations.keys() and configurations['fillna'] is not None:
                fillna = configurations['fillna']
                assert type(fillna) in [str, int, float], "The fillna value must be a string, float or integer"
            elif col_with_values == '':
                fillna = 0
            else:
                fillna = None

            # Check if the user gives pre_name parameter
            # if not the pre_name will be the original value name in col_to_transpose
            if 'pre_name' in configurations.keys() and configurations['pre_name'] is not None:
                # Verify type
                assert type(configurations['pre_name']) == str, "The pre_name parameter must be a string"
                # Verify length of pre_name
                pre_name = configurations['pre_name'].split(',')
                assert len(pre_name) == len(col_to_transpose), \
                    "The pre_name parameter must have the length: {} (the same as col_to_transpose)" \
                    .format(str(len(col_to_transpose)))
            else:
                pre_name = col_to_transpose

            # Verify if the user gives the agg_func parameter
            # if not it will be an empty string
            if 'agg_func' in configurations.keys() and configurations['agg_func'] is not None:
                agg_func = configurations['agg_func']
                # Check type
                assert type(agg_func) == str, "The agg_func parameter must be a string"
            else:
                agg_func = ''

            for col, pre_col_name in zip(col_to_transpose, pre_name):
                # Verify if the values in categories_to_col are present in the column
                # col_to_transpose in the dataframe
                # if they are not present give a warning to the user and remove them from categories_to_col
                # if the user didn't give the categories_to_col then all the categories present in the column will
                # be considered

                if len(original_categories_to_col) != 0:
                    missing_values = [x for x in original_categories_to_col if x not in df[col].values.tolist()]
                    if len(missing_values) != 0:
                        logger.warning("The values {} in categories_to_col are not present in df. "
                                       "They will not be converted into col"
                                       .format(', '.join([str(x) for x in missing_values])))

                    categories_to_col = [col for col in original_categories_to_col if col not in missing_values]
                else:
                    categories_to_col = df[col].drop_duplicates().values.tolist()

                # Strip the values of the column to transpose
                # Change type of the col_to_transpose to string
                df.loc[:, col] = df.loc[:, col].astype(str)
                df.loc[:, col] = df.loc[:, col].str.strip()

                # categories_to_col must be strings
                # Change categories_to_col to string
                categories_to_col = [str(v) for v in categories_to_col]
                # Strip the values of categories_to_col and remove duplicates
                categories_to_col = [value.strip() for value in categories_to_col]
                categories_to_col = list(set(categories_to_col))

                # pivot the column in df
                # Check if the user gives the col_with_values
                if col_with_values == '':
                    df.insert(1, 'indicator', 1)
                    col_with_values = 'indicator'

                # Restrict the values of a column by the categories_to_col
                stay_cols = id_col.copy()
                stay_cols.append(col)
                stay_cols.append(col_with_values)
                restricted_df = df.copy()
                restricted_df = restricted_df.loc[restricted_df[col].isin(categories_to_col), stay_cols]

                # Verify if agg_func was given (that is agg_func != '')
                # if True use pd.pivot_table with that parameter
                # if not verify col and index have duplicated entries and then use pd.pivot
                if agg_func != '':
                    # Check type
                    assert type(agg_func) == str, "The agg_func parameter must be a string"
                    df_transpose = restricted_df.pivot_table(index=id_col, columns=col,
                                                      values=col_with_values, aggfunc=agg_func)
                else:
                    # if col and index have duplicated entries the pivot will give an error:
                    # " Index contains duplicate entries, cannot reshape "
                    # For better understand we will give an error for the user to consider
                    # making a group by before using this function or give the agg_func option as a parameter of
                    # this function
                    cols_to_check = id_col.copy()
                    cols_to_check.append(col)
                    if_duplicated = restricted_df[cols_to_check].duplicated()
                    assert all(~if_duplicated), \
                        "There are duplicated entries in [{}, {}]. Consider making a group by before using this " \
                        "function or give the agg_func option as a parameter for this function" \
                        .format(id_col, col)

                    df_transpose = restricted_df.pivot(index=id_col, columns=col, values=col_with_values)

                # Verify if the user
                if fillna is not None:
                    df_transpose = df_transpose.fillna(fillna)

                # Rename columns
                rename_dict = {k: '{}_{}'.format(pre_col_name, k) for k in categories_to_col}
                new_cols = ['{}_{}'.format(pre_col_name, x) for x in categories_to_col]
                df_transpose = df_transpose.rename(columns=rename_dict, inplace=False)

                # Add the new columns from pivoting to the dataframe
                df = pd.merge(
                    df,
                    df_transpose[new_cols],
                    on=id_col,
                    how='left'
                )
                # Remove the indicator
                if col_with_values == '':
                    df = df.drop(columns=['indicator'])

                # If remove_col_to_transpose in configurations keys and is True
                # the col_to_transpose will be removed from the dataframe
                # otherwise maintain the column in the resulted dataframe
                if 'remove_col_to_transpose' in configurations.keys():
                    remove_col_to_transpose = configurations['remove_col_to_transpose']
                    assert type(remove_col_to_transpose) == bool, \
                        "The remove_col_to_transpose key in configurations must be a boolean"
                    if remove_col_to_transpose:
                        df = df.drop(columns=[col])

                # Remove potential duplicate rows
                df = df.drop_duplicates()

            return df
        except Exception as e:
            logger.error(e)

    @staticmethod
    def concatenate_cols(df: pd.DataFrame, **configurations) -> pd.DataFrame:
        """
        Function to concatenate multiple columns into just one column that is added to the base dataframe
        :param df: base dataframe
        :param configurations: dictionary with configurations needed
            columns: string with the columns that will be concatenated
            col_name: name of the column that will be added to the dataframe
            remove_cols: boolean. If True, remove the cols in columns from the dataframe
        :return: dataframe containing the new column
        """
        try:
            logger.info("Start concatenate_cols transformation")

            # verify input variables
            assert type(df) == pd.DataFrame, "The df parameter must be a DataFrame"
            assert type(configurations) == dict, "The configurations must be a dictionary"

            # verify mandatory keys
            missing_keys = [x for x in ['columns', 'col_name'] if x not in configurations.keys()]
            assert len(missing_keys) == 0, \
                "The keys {} are missing from configurations".format(', '.join(missing_keys))

            # verify keys types
            col_name = configurations['col_name']
            assert type(col_name) == str, "The col_name parameter must be a string"
            cols = configurations['columns']
            assert type(cols) == str, "The columns parameter must be a string"
            selected_columns = cols.split(',')
            if 'remove_cols' in configurations:
                assert type(configurations['remove_cols']) == bool, "The remove_cols parameter must be a boolean"

            # verify if the cols in the columns parameter are columns of the dataframe
            missing_cols = [col for col in selected_columns if col not in df.columns]
            assert len(missing_cols) == 0, "{} in the columns parameter are not columns of the dataframe". \
                format(', '.join(missing_cols))

            # convert every col in columns to type str
            for col in selected_columns:
                df[col] = df[col].astype(str)

            # insert the new column into the dataframe
            df.insert(loc=df.shape[1], column=col_name, value=df[selected_columns].sum(axis=1, numeric_only=False))

            # if remove_cols parameter is in configurations and if it is true,
            # remove cols in columns parameter from the dataframe
            if 'remove_cols' in configurations.keys() and configurations['remove_cols']:
                df.drop(selected_columns, inplace=True, axis=1)

            return df
        except Exception as e:
            logger.error(e)
            raise e

    @staticmethod
    def if_in_category(df: pd.DataFrame, **configurations) -> pd.DataFrame:
        """
        Function to change date_col to a given format
        :param df: data frame
        :type df: pandas.DataFrame
        :param configurations: dictionary with configurations needed
        :type configurations: dict
        :return: changed DataFrame
        :rtype: pandas.DataFrame
        """
        try:
            logger.info("Start if_in_category transformation")

            # verify input variables
            assert type(df) == pd.DataFrame, "The df parameter must be a DataFrame"
            assert type(configurations) == dict, "The configurations must be a dictionary"
            # Verify 'cols' key in configurations
            assert 'cols' in configurations.keys(), "The key 'cols' is missing from configurations"
            assert type(configurations['cols']) == dict, \
                "The key 'cols' value must be a dictionary, which key is a column name"

            new_cols_names = []
            # Verify if the values for the keys in configurations['cols'] are lists
            assert_type = [k for k in configurations['cols'].keys()
                           if type(configurations['cols'][k]) != list]
            assert len(assert_type) == 0, "The value of {} must be list".format(', '.join(assert_type))

            for k in configurations['cols'].keys():
                # Verify if the columns to categorize have tags
                cols_to_categorize = AuxiliaryFunctions.verify_and_choose_columns_by_tag(df, [k])
                # Verify if all cols are in the dataframe
                missing_cols = [col for col in cols_to_categorize if col not in df.columns.tolist()]
                assert len(missing_cols) == 0, \
                    "The columns {} are missing from dataframe.".format(', '.join(missing_cols))

                # Verify values

                for col in cols_to_categorize:
                    name = "{}_categorized".format(col)
                    df.loc[:, name] = 0
                    df.loc[df[col].isin(configurations['cols'][k]), name] = 1
                    new_cols_names.append(name)

            df = AuxiliaryFunctions.add_date_end_colname(df, cols_to_verify=new_cols_names)
            return df
        except Exception as e:
            logger.error(e)

    @classmethod
    def subtract_cols(cls, df: pd.DataFrame, **configurations):
        """
        Subtract cols to cols_to_subtract columns
        :param df:
        :param configurations:
        :return:
        """
        try:
            logger.info('Started subtract_cols')

            # Verify input type
            assert type(df) == pd.DataFrame, "The df parameter must be a DataFrame"
            assert type(configurations) == dict, "The configurations must be a dict"

            # Verify if input configurations have the needed keys
            needed_keys = ['cols', 'cols_to_subtract']
            missing_keys = [k for k in needed_keys if k not in configurations.keys()]
            assert len(missing_keys) == 0, \
                "The keys {} are missing from configurations".format(', '.join(missing_keys))

            # Verify types:
            assert type(configurations['cols']) == str, \
                "The cols in configurations must be a string with the columns separated by commas."
            assert type(configurations['cols_to_subtract']) == str, \
                "The cols_to_subtract in configurations must be a string with the columns separated by commas."

            cols = configurations['cols'].split(',')
            cols_to_subtract = configurations['cols_to_subtract'].split(',')

            # Verify if length of cols and cols_to_subtract is equal
            assert len(cols) == len(cols_to_subtract), \
                "The length of cols and cols_to_subtract must be equal: {}!={}".format(len(cols), len(cols_to_subtract))

            # Verify if cols and cols_to_subtract are in the dataframe
            verify_cols = [col for col in cols if col not in df.columns.tolist()]
            assert len(verify_cols) == 0, \
                'The columns {} are not present in df'.format(', '.join(verify_cols))
            verify_cols_to_subtract = [col for col in cols_to_subtract if col not in df.columns.tolist()]
            assert len(verify_cols_to_subtract) == 0, \
                'The columns {} are not present in df'.format(', '.join(verify_cols_to_subtract))

            # Check if the user give the name to give to the resulting columns
            # The diff_cols_name must have the same length as the cols and cols_to_subtract
            if 'diff_cols_name' in configurations.keys():
                assert type(configurations['diff_cols_name']) != list, \
                    "The diff_cols_name must not be a list"
                diff_cols_name = configurations['diff_cols_name'].split(',')
                assert len(diff_cols_name) == len(cols), \
                    "The length of the diff_cols_name id different from cols. {}!={}" \
                    .format(len(diff_cols_name), len(cols))
            else:
                diff_cols_name = ['diff_{}_by_{}'.format(col, sub_col) for col, sub_col in zip(cols, cols_to_subtract)]

            # Check if the user give the type to convert the columns, if not assume float
            if 'cols_type' in configurations.keys():
                assert type(configurations['cols_type']) == str, \
                    "The cols_type in configurations must be a string"
                cols_type = configurations['cols_type']
            else:
                cols_type = 'float'

            # Subtract by order the cols_to_subtract to the cols
            for i in range(0, len(cols)):
                col = cols[i]
                sub_col = cols_to_subtract[i]

                # This part is to avoid the error: invalid literal for int()
                # That is if the number given is in string: '2.4' and you give cols_type = int
                # pd.astype cannot convert, need to convert first to float and then to int
                df.loc[:, col] = df.loc[:, col].astype(float)
                df.loc[:, sub_col] = df.loc[:, sub_col].astype(float)

                if cols_type != 'float':
                    df.loc[:, col] = df.loc[:, col].astype(cols_type)
                    df.loc[:, sub_col] = df.loc[:, sub_col].astype(cols_type)

                df.loc[:, diff_cols_name[i]] = df[col] - df[sub_col]

            return df
        except Exception as e:
            logger.error(e)

    @classmethod
    def subtract_cols_by_date_tag(cls, df: pd.DataFrame, **configurations) -> pd.DataFrame:
        """
        Subtract columns with a tag by date_tag
        :param df:
        :param configurations:
        :return:
        """
        try:
            logger.info('Started subtract_cols_by_date_tag')
            # Verify input type
            assert type(df) == pd.DataFrame, "The df parameter must be a DataFrame"
            assert type(configurations) == dict, "The configurations must be a dict"

            # Verify input configurations
            needed_keys = ['cols', 'date_format']
            missing_keys = [k for k in needed_keys if k not in configurations.keys()]
            assert len(missing_keys) == 0, \
                "The keys {} are missing from configurations".format(', '.join(missing_keys))

            # Verify types of the keys in configurations
            assert type(configurations['cols']) == str, \
                "The cols in configurations must be a string with the columns separated by commas."
            assert type(configurations['date_format']) == str, \
                "The date_format parameter must be a string"
            cols = configurations['cols'].split(',')
            date_format_cols = configurations['date_format']

            # Verify if the user gives the diff_cols_name parameter
            # if not the subtracted column names will be 'diff_{cols}'
            if 'diff_cols_name' in configurations.keys():
                assert type(configurations['diff_cols_name']) == str, \
                    "The cols in configurations must be a string with the columns separated by commas."
                diff_cols_name = configurations['diff_cols_name'].split(',')
                assert len(diff_cols_name) == len(cols), \
                    "The diff_cols_name must be the same number of columns as cols"
            else:
                diff_cols_name = ['diff_{}'.format(col) for col in cols]
                assert len(set(diff_cols_name)) == len(diff_cols_name), \
                    "There will be more than one diff col with the same name. " \
                    "Consider to give the diff_cols_name parameter"

            # Verify substitution_tag is present in configurations
            # if it is not present assume subs_tag_cols to be equal to '#all-cols-contain#'
            # Verify that substitution tags are present in possible_tags
            possible_tags = ['#all-cols-starts-with#', "#all-cols-middle-with#", "#all-cols-contain#"]
            if 'subs_tag_cols' in configurations.keys():
                assert type(configurations['subs_tag_cols']) == str, \
                    "The substitution_tag in configurations must be a string with the columns separated by commas."
                subs_tag_cols = configurations['subs_tag_cols']
                # The date must be in the end of the column so that implies that the only substitution tags possible
                # are: '#all-cols-contain#'; '#all-cols-starts-with#'; '#all-cols-middle-with#'
                assert subs_tag_cols in possible_tags, \
                    "The date must be in the end of the column so that implies that the only substitution tags\
                     possible are: #all-cols-contain#; #all-cols-starts-with#; #all-cols-middle-with#"
            else:
                subs_tag_cols = '#all-cols-contain#'

            # Verify if col_type in configuration keys
            # if not assume to be float
            if 'cols_type' in configurations.keys():
                assert type(configurations['cols_type']) == str, \
                    "The cols_type in configurations must be a string"
                cols_type = configurations['cols_type']
            else:
                cols_type = 'float'

            # Verify if cols_to_subtract is present in the configurations
            # if it is True then they must have the same number of elements as cols
            # And the subtraction will be made based on the date of the columns by the order, that is,
            # cols[0] - cols_to_subtract[0] and so on
            if 'cols_to_subtract' in configurations.keys():
                # Check type
                assert type(configurations['cols_to_subtract']) == str, \
                    "The cols_to_subtract parameter must be a string with the columns separated by commas."

                # Compare cols_to_subtract length with cols length
                sub_cols = configurations['cols_to_subtract'].split(',')
                assert len(cols) == len(sub_cols), \
                    "The cols and cols_to_subtract parameter must have the same length"

                # Verify subs_tag_subtract_cols is present in configurations
                # if it is not present assume subs_tag_subtract_cols to be equal to '#all-cols-contain#'
                if 'subs_tag_subtract_cols' in configurations.keys():
                    assert type(configurations['subs_tag_subtract_cols']) == str, \
                        "The subs_tag_subtract_cols in configurations must be a string with " \
                        "the substitution_tag desired"
                    subs_tag_sub_cols = configurations['subs_tag_subtract_cols']
                    # The date must be in the end of the column so that implies that the only substitution tags possible
                    # are: '#all-cols-contain#'; '#all-cols-starts-with#'; '#all-cols-middle-with#'
                    assert subs_tag_sub_cols in possible_tags, \
                        "The date must be in the end of the column so that implies that the only substitution tags\
                         possible are: #all-cols-contain#; #all-cols-starts-with#; #all-cols-middle-with#"

                else:
                    subs_tag_sub_cols = '#all-cols-contain#'

                # Verify if date_format_subtract_cols is present in configurations
                # if it is not present date_format_subtract_cols = date_format
                if 'date_format_subtract_cols' in configurations.keys():
                    assert type(configurations['date_format_subtract_cols']) == str, \
                        "The date_format parameter must be a string"
                    date_format_subtract_cols = configurations['date_format_subtract_cols']
                else:
                    date_format_subtract_cols = date_format_cols

            # if cols_to_subtract is not present everything will be put in the default values:
            else:
                sub_cols = ['None'] * len(cols)
                date_format_subtract_cols = date_format_cols
                subs_tag_sub_cols = subs_tag_cols

            for j in range(0, len(cols)):
                col = cols[j]
                sub_col = sub_cols[j]
                cols_to_be_subtracted = []
                # Get all columns correspondent to cols with the respective substitution_tag
                col_with_tag = ['{}{}'.format(col, subs_tag_cols)]
                col_with_tag = AuxiliaryFunctions.verify_and_choose_columns_by_tag(df, col_with_tag)

                # Verify that col_with_tag is not empty
                assert len(col_with_tag) > 0, \
                    "There weren't found any correspondent columns {} for the substitution tag {}" \
                    .format(col, subs_tag_cols)

                # Get the dates in the columns col_with_tag
                cols_date = ['_'.join(col.split('_')[-(date_format_cols.count('_') + 1):])
                             for col in col_with_tag]
                date_format = [date_format_cols]
                nr_parts_date = [date_format_cols.count('_')]
                cols_to_be_subtracted.extend(col_with_tag)

                if sub_col != 'None':
                    sub_col_with_tag = ['{}{}'.format(sub_col, subs_tag_sub_cols)]
                    sub_col_with_tag = AuxiliaryFunctions.verify_and_choose_columns_by_tag(df, sub_col_with_tag)
                    # Verify that sub_col_with_tag is not empty
                    assert len(sub_col_with_tag) > 0, \
                        "There weren't found any correspondent columns {} for the substitution tag {}" \
                        .format(sub_col, subs_tag_sub_cols)

                    cols_to_be_subtracted.extend(sub_col_with_tag)

                    # Get the dates in the columns col_with_tag
                    nr_parts_date.append(date_format_subtract_cols.count('_'))
                    date_format.append(date_format_subtract_cols)
                    cols_date.extend(['_'.join(sub_col.split('_')[-(nr_parts_date[1] + 1):])
                                      for sub_col in sub_col_with_tag])
                else:
                    date_format.append(date_format_cols)
                    nr_parts_date.append(date_format_cols.count('_'))

                nr_col = len(cols_to_be_subtracted)
                if nr_col > 2:
                    # if length of cols_to_be_subtracted is greater than 2 then check which
                    # get the similarity between col_with_tag and col
                    # get the similarity between sub_col_with_tag and sub_col
                    # and then select the more similar
                    cols_without_tags = [col.replace(cols_date[i], '')
                                         for (col, i) in zip(cols_to_be_subtracted, range(0, nr_col))]

                    similarity_rate_col = [SequenceMatcher(None, c, col).ratio()
                                           for c in cols_without_tags]

                    most_similar_cols = [cols_to_be_subtracted[i]
                                         for i in range(0, nr_col)
                                         if similarity_rate_col[i] == max(similarity_rate_col)]

                    if sub_col != 'None':
                        similarity_rate_sub_col = [SequenceMatcher(None, c, sub_col).ratio()
                                                   for c in cols_without_tags]
                        most_similar_sub_cols = [cols_to_be_subtracted[i]
                                                 for i in range(0, nr_col)
                                                 if similarity_rate_sub_col[i] == max(similarity_rate_sub_col)]

                        cols_to_be_subtracted = most_similar_cols
                        cols_to_be_subtracted.extend(most_similar_sub_cols)

                    else:
                        cols_to_be_subtracted = most_similar_cols

                    # Check that cols_to_be_subtract have two and only two columns
                    assert len(cols_to_be_subtracted) == 2, \
                        "The cols_to_be_subtracted most have two and only tow columns. Columns identified: {}.\
                        Case: cols = {} and sub_cols = {}" \
                            .format(', '.join(cols_to_be_subtracted), col, sub_col)

                    # Verify that the columns you are subtracting are present in the dataframe
                    missing_cols = [col for col in cols_to_be_subtracted if col not in df.columns.tolist()]
                    assert len(missing_cols) == 0, \
                        "The columns {} are missing from the dataframe".format(', '.join(missing_cols))

                # Get dates associated with the columns
                # Note the date of the column must be at the end of the column name
                date0 = '_'.join(cols_to_be_subtracted[0].split('_')[-(nr_parts_date[0] + 1):])
                date0 = datetime.strptime(date0, date_format[0])

                date1 = '_'.join(cols_to_be_subtracted[1].split('_')[-(nr_parts_date[1] + 1):])
                date1 = datetime.strptime(date1, date_format[1])

                # Define the input to subtract_cols function
                # That is consider the recent date to be cols and the older to be cols_to_subtract
                if date0 > date1:
                    conf = {
                        'cols': cols_to_be_subtracted[0],
                        'cols_to_subtract': cols_to_be_subtracted[1],
                        'cols_type': cols_type
                    }
                elif date1 > date0:
                    conf = {
                        'cols': cols_to_be_subtracted[1],
                        'cols_to_subtract': cols_to_be_subtracted[0],
                        'cols_type': cols_type
                    }
                else:
                    raise Exception("The dates in the column given are the same {}".format(date1))

                logger.info('Subtracting column {} to {}'.format(conf['cols'], conf['cols_to_subtract']))

                conf['diff_cols_name'] = diff_cols_name[j]
                df = cls.subtract_cols(df, **conf)

            return df
        except Exception as e:
            logger.error(e)

    @staticmethod
    def create_indicator(df, **configurations) -> pd.DataFrame:
        """
        Create indicator based on queries
        :param df:
        :param configurations:
        :return:
        """
        try:
            logger.info('Start create_indicator')
            # verify input
            assert type(df) == pd.DataFrame, "The df parameter must be a dataframe"
            assert type(configurations) == dict, "The configurations parameter must be a dictionary"

            # Verify if the needed keys are present in configurations
            needed_keys = ['default_value', 'col_name']
            missing_keys = [k for k in needed_keys if k not in configurations.keys()]
            assert len(missing_keys) == 0, \
                "The keys are missing from configurations".format(', '.join(missing_keys))

            default_value = configurations['default_value']
            col_name = configurations['col_name']
            # verify types:
            assert type(col_name) == str, "The col_name parameter must be a string"

            # Verify if the user gives at least one 'value' key
            indicator_sequence = [k for k in configurations.keys() if k.startswith('value')]
            assert len(indicator_sequence) != 0, \
                "The configurations must have at least one key starting with 'value', that is, 'value#number'"

            # Create the column that will contain the filter with the default value = default_value
            df_indicator = df.copy()
            if type(default_value) != str:
                df_indicator.insert(df_indicator.shape[1], col_name, default_value)
            if type(default_value) == str:
                default_value = default_value.split(':')
                if len(default_value) == 1:
                    df_indicator.insert(df_indicator.shape[1], col_name, default_value[0])
                elif len(default_value) == 2:
                    df_indicator.insert(df_indicator.shape[1], col_name, df_indicator[default_value[1]])

            # Get filters and the respective values
            for k in indicator_sequence:
                # Verify that the key 'value' is always present
                assert 'value' in configurations[k].keys(), "The 'value' key is missing from {}".format(k)
                ind_value = configurations[k]['value']
                # Verify that the key 'filter' is always present
                assert 'query' in configurations[k].keys(), "The 'query' key is missing from {}".format(k)
                ind_query = configurations[k]['query']
                # verify if the filter is a string
                assert type(ind_query) == str, "The 'filter' value in {} must be a string".format(k)

                # in case of existing sub_tags, that is, exist_subs_tags key
                # then start the substitution of the columns by the respective columns in the query
                if 'exist_subs_tags' in configurations[k].keys() and configurations[k]['exist_subs_tags'] is not None:
                    # if exist_subs_tags in configurations subtitution of col_tags is needed
                    # We will need to sp0lit the query by curve commas
                    # Note: The subqueries must be separated by curve brackets
                    # 2. Split the ind_query by '(' and ')'
                    split_query = re.split("[()]", ind_query)
                    for sub_tag in configurations[k]['exist_subs_tags'].keys():
                        sub_tag_params = configurations[k]['exist_subs_tags'][sub_tag]

                        # Verify if there are the needed keys for the case of exist_sub_tags
                        needed_keys = ['substitution_tag', 'operation']
                        missing_keys = [k for k in needed_keys if k not in sub_tag_params.keys()]
                        assert len(missing_keys) == 0, \
                            "The keys are missing from the params in the exist_sub_tags {}" \
                                .format(', '.join(needed_keys))

                        # Check the params types
                        assert type(sub_tag_params['substitution_tag']) == str, \
                            "The 'sub_tag' parameter must be a string"
                        assert type(sub_tag_params['operation']) == str, \
                            "The 'operation' parameter must be a string"

                        # Verify if sub_tag is always valid
                        substitution_tags = ['#all-cols-starts-with#', '#all-cols-ends-with#',
                                             "#all-cols-middle-with#", "#all-cols-contain#"]
                        assert sub_tag_params['substitution_tag'] in substitution_tags, \
                            "The substitution_tag given {} is not valid. Check the list: {}" \
                            .format(sub_tag_params['substitution_tag'], ', '.join(substitution_tags))

                        # Verify if col is present in sub_tag_params
                        # if not the default will be the key sub_tag
                        if 'col' in sub_tag_params.keys():
                            # Verify type
                            assert type(sub_tag_params['col']) == str, "The 'col' parameter must be a string"
                            sub_col_tag = sub_tag_params['col']
                        else:
                            sub_col_tag = sub_tag

                        # Start substitution in the query
                        # Note: in this case the queries must be separated by curve brackets
                        # 1. check the columns correspondent to sub_col_tag
                        cols_with_tag = AuxiliaryFunctions.verify_and_choose_columns_by_tag(
                            df,
                            ['{}{}'.format(sub_col_tag, sub_tag_params['substitution_tag'])])
                        # verify if cols_with_tag is not empty
                        assert len(cols_with_tag) != 0, "There were not correspondent cols for {}".format(
                            sub_col_tag)

                        # 2. Start substitution
                        logger.info('Substituting in the query the col_tag: {} for the respective columns {}'
                                    .format(sub_tag, ','.join(cols_with_tag)))

                        # 3.1 find in split_query the occurrence of sub_tag and substitute
                        # a) until the last col_with_tag substitute sub_tag by:
                        # new_col_name + [the rest of the query] + operation
                        # b) in the last one substitute by: new_col_name + [the rest of the query]
                        # Note the space in '{} '.format(sub_tag) is to have certain that we are substituting
                        # in the desired tag
                        ind = [i for i in range(0, len(split_query)) if '{} '.format(sub_tag) in split_query[i]][0]
                        pre_result = [
                            '({}) {} '.format(split_query[ind].replace(sub_tag, '`{}`'.format(cols_with_tag[i])),
                                              sub_tag_params['operation'])
                            if i < (len(cols_with_tag) - 1)
                            else '({})'.format(split_query[ind].replace(sub_tag, '`{}`'.format(cols_with_tag[i])))
                            for i in range(0, len(cols_with_tag))
                        ]

                        split_query[ind] = "({})".format(''.join(pre_result))
                    # When the cycle finish join all the sub queries again
                    ind_query = ' '.join(split_query)

                # Filter the dataframe accordingly with the filter given
                logger.info('Apply condition: {} with value {}'.format(ind_query, ind_value))
                df_filter = df_indicator.query(ind_query)
                df_indicator.loc[df_filter.index, col_name] = ind_value

            return df_indicator

        except Exception as e:
            logger.error(e)
            raise Exception(e)

    @staticmethod
    def sort_column(df: pd.DataFrame, **configurations) -> pd.DataFrame:
        """
        Function to rank a sort the values of a column
        :param df: data frame
        :type df: pandas.DataFrame
        :param configurations: dictionary with configurations needed
        :type configurations: dict
        :return: changed DataFrame
        :rtype: pandas.DataFrame
        """
        try:
            logger.info("Start sort_column transformation")
            # verify input variables
            assert type(df) == pd.DataFrame, "The df parameter must be a DataFrame"
            assert type(configurations) == dict, "The configurations must be a dictionary"

            # verify mandatory keys
            missing_keys = [x for x in ['cols', 'ascending'] if x not in configurations.keys()]
            assert len(missing_keys) == 0, \
                "The keys {} are missing from configurations".format(', '.join(missing_keys))
            # Verify cols
            assert type(configurations['cols']) == str, "The cols parameter must be a string"
            cols = configurations['cols'].split(',')
            cols = AuxiliaryFunctions.verify_and_choose_columns_by_tag(df, cols)
            missing_cols = [col for col in cols if col not in df.columns.tolist()]

            assert len(missing_cols) == 0, \
                "The columns {} are missing from the dataframe".format(', '.join(missing_cols))

            # Verify ascending
            ascending = configurations['ascending']
            assert type(ascending) == bool, "The ascending parameter must be a boolean"

            # Sort the values of the dataframe by the column given
            df = df.sort_values(cols, ascending=ascending, axis=0)

            return df
        except Exception as e:
            logger.error(e)

    @classmethod
    def order_by_cols(cls, df: pd.DataFrame, **configurations) -> pd.DataFrame:
        """
        Function to rank a column dataframe according to another given co
        :param df: data frame
        :type df: pandas.DataFrame
        :param configurations: dictionary with configurations needed
        :type configurations: dict
        :return: changed DataFrame
        :rtype: pandas.DataFrame
        """
        try:
            logger.info("Start order_by_col transformation")
            # verify input variables
            assert type(df) == pd.DataFrame, "The df parameter must be a DataFrame"
            assert type(configurations) == dict, "The configurations must be a dictionary"
            # verify mandatory keys
            missing_keys = [x for x in ['column', 'ascending', 'dependence'] if x not in configurations.keys()]
            assert len(missing_keys) == 0, \
                "The keys {} are missing from configurations".format(', '.join(missing_keys))
            # Check if the parameter columns are in the dataframe
            assert configurations['column'] in df.columns, "The column is not in the dataframe"
            assert configurations['dependence'] in df.columns, "The column is not in the dataframe"
            # Check if the parameter columns are strings
            assert type(configurations['column']) == str, "The column is not a string"
            assert type(configurations['dependence']) == str, "The column is not a string"
            # Check ascending parameter options
            assert configurations['ascending'] in [True, False], "The ascending parameter options are wrong"
            column = configurations['column']
            dependence = configurations['dependence']
            ascending = configurations['ascending']

            # Verify if the user had given the new_column_name parameter
            if 'new_column_name' in configurations.keys() and configurations['new_column_name'] is not None:
                column_name = configurations['new_column_name']
                assert type(column_name) == str, "The column_name parameter must be a string"
            else:
                column_name = 'rank_{}_by_{}'.format(column, dependence)

            # Verify if the the user had given the method parameter
            if 'method_ties' in configurations.keys() and configurations['method_ties'] is not None:
                method_ties = configurations['method_ties']
                assert type(method_ties) == str, "The method_ties parameter must be a string"
                possible_method_ties = ['average', 'min', 'max', 'first', 'dense']
                assert method_ties in possible_method_ties, \
                    "The method_ties available are {}".format(', '.join(possible_method_ties))
            else:
                method_ties = 'average'
            dependence_list = df[dependence].drop_duplicates()
            # Multiprocessing
            func = partial(cls.order_func, df, dependence, column_name, column, ascending, method_ties)
            pool = mp.Pool(mp.cpu_count() - 1)
            results = pool.map(func, dependence_list)
            pool.close()
            pool.join()
            results_df = pd.concat(results)

            return results_df

        except Exception as e:
            logger.error(e)

    @classmethod
    def order_func(cls, df: pd.DataFrame, dependence: str, column_name: str, column: str, ascending: str,
                   method_ties: str, element: str) -> pd.DataFrame:
        """
        Function to  to a given format
        :param method_ties:
        :param element:
        :param column:
        :param ascending:
        :param column_name:
        :param dependence:
        :param df: data frame
        :type df: pandas.DataFrame
        :return: changed DataFrame
        :rtype: pandas.DataFrame
        """
        # Apply the rank
        data = df[df[dependence] == element].copy()
        data.loc[:, column_name] = data.loc[:, column].rank(ascending=ascending, method=method_ties)
        return data

    @staticmethod
    def concatenate(df, **configurations) -> pd.DataFrame:
        """
        Concatenate a aux_source to the base dataframe
        :param df: base dataframe
        :param configurations: dictionary with all input variables
        :return: changed dataframe with new columns
        """
        try:
            logger.info('Started concatenate')
            # Verify input
            assert type(df) == pd.DataFrame, "The df parameter is not a DataFrame."

            # Check configurations content: if the necessary keys are present
            allowed_keys = ['aux_source', 'axis']
            verify_keys = [x for x in allowed_keys if x not in configurations.keys()]
            assert len(verify_keys) == 0, "The parameters: {} are missing".format(", ".join(verify_keys))

            aux_source_tag = configurations['aux_source']
            concat_axis = configurations['axis']
            # Check configurations content: if the auxiliary source was loaded and is present in configurations
            assert aux_source_tag in configurations.keys(), \
                "The auxiliary source {} is missing".format(aux_source_tag)
            aux_source = configurations[aux_source_tag]

            # Check configurations content: check type of the content
            assert type(aux_source) == pd.DataFrame, "The aux_source parameter must be a DataFrame"
            assert type(concat_axis) == int, "The axis parameter must be an integer"
            assert concat_axis in [0, 1], "The axis parameter must be 0 or 1, " \
                                          "for concatenation of rows or columns, respectively"

            if concat_axis == 1:
                if 'cols_aux_to_add' in configurations.keys() and configurations['cols_aux_to_add'] is not None:
                    cols_aux_to_add = configurations['cols_aux_to_add']
                    # cols_aux_to_add
                    assert type(cols_aux_to_add) == str, \
                        "The cols_aux_to_add parameter must be str. If more than one separate them by comma"
                    cols_aux_to_add = cols_aux_to_add.split(',')

                    # Verify if the columns have substitution tags
                    cols_aux_to_add = AuxiliaryFunctions.verify_and_choose_columns_by_tag(aux_source, cols_aux_to_add)

                    # Check configurations content: Check if the col_aux and cols_aux_to_add are present in aux_source
                    missing_cols = [col for col in cols_aux_to_add if col not in aux_source.columns.tolist()]
                    assert len(missing_cols) == 0, \
                        'Columns {} are not in the {}'.format(', '.join(missing_cols), aux_source_tag)
                else:
                    cols_aux_to_add = aux_source.columns.tolist()

                # Get only cols_aux_to_add in aux_source
                aux_source = aux_source[cols_aux_to_add]

                # Check if the user give a pre_name parameter
                if 'pre_name' in configurations.keys() and configurations['pre_name'] is not None:
                    # Check if it is a string
                    assert type(configurations['pre_name']) == str, "The pre_name parameter must be a string"

                    # Apply pre_name
                    aux_source.columns = ['{}_{}'.format(configurations['pre_name'], col)
                                          for col in cols_aux_to_add]
                else:
                    # If the user doesn't give a pre_name
                    # Verify if there are columns with the same the name and warn the user
                    matching_cols = [col for col in cols_aux_to_add if col in df.columns.tolist()]
                    if len(matching_cols) != 0:
                        logger.warning("The auxiliary source and base dataframe have columns in common: {}."
                                       "Please give a pre_name parameter"
                                       .format(matching_cols))

            df = pd.concat([df, aux_source], axis=concat_axis)

            # Verify if the user had given the parameter remove_duplicates
            if 'remove_duplicates' in configurations.keys() and configurations['remove_duplicates'] is not None:
                # Check if it is a string
                assert type(configurations['remove_duplicates']) == bool, \
                    "The remove_duplicates parameter must be a boolean"
                remove_duplicates = configurations['remove_duplicates']
            else:
                remove_duplicates = True

            # Apply remove duplicates
            if remove_duplicates:
                df = df.drop_duplicates()

            return df
        except Exception as e:
            logger.error(e)
            raise e

    @staticmethod
    def cells_rename(df: pd.DataFrame, **configurations) -> pd.DataFrame:
        """
        This function renames the cells of a column
        :param df: Receives a DataFrame
        """
        # Check input
        assert type(df) == pd.DataFrame, "The df parameter is not a DataFrame."
        assert 'new_cell_names' in configurations.keys(), "The new_cell_names parameter is not given by the user"
        assert 'col' in configurations.keys(), "The col parameter is not given by the user"

        # Check types
        new_cell_names = configurations['new_cell_names']
        assert type(new_cell_names) == dict, \
            "The new_cell_names parameter must be a dictionary. " \
            "Where the keys are the original names and the values the new ones"

        col = configurations['col']
        assert type(col) == str, "The col parameter must be a string."
        assert col in df.columns.tolist(), "The column {} is missing from the dataframe".format(col)

        # Verify if the columns have substitution_tags
        verify_list = AuxiliaryFunctions.verify_tags_in_list(
            df[col].drop_duplicates().values.tolist(), list(new_cell_names.keys()), return_dict=True)

        # Reformulate the cells expected with the verify_list result
        # Substitute the key with tag replacing it by the new cols values
        cells_expected = dict([
            (k1, k1.replace(k.replace(tag, ''), v))
            for k, v in new_cell_names.items()
            for k1 in verify_list[k]['values']
            for tag in verify_list[k]['tag']
        ])

        # Check if the values are present in the dataframe if not warn the user
        missing_values = [k for k in cells_expected.keys() if k not in df[col].values.tolist()]
        if len(missing_values) != 0:
            logger.warning("There are no values: {} not present in the column {}."
                           "They will be removed from the new_cell_names".format(', '.join(missing_values), col))
            cells_expected = {k: v for k, v in cells_expected.items() if k not in missing_values}

        if 'new_col' in configurations.keys():
            new_col = configurations['new_col']
            assert type(new_col) == str, "The new_col parameter must be a dictionary"
        else:
            new_col = col
        logger.info(cells_expected)
        df.loc[:, new_col] = df[col].replace(cells_expected)

        return df


class Indicators:
    """
    Class methods related with indicators
    """

    @staticmethod
    def if_present(df: pd.DataFrame, **configurations):
        """

        :param df:
        :param configurations:
        :return:
        """
        source_aux_tag = configurations['aux_source']
        aux_source = configurations[source_aux_tag]
        col_name = configurations['new_col_name']
        col_base = configurations['col_base']
        col_aux = configurations['col_aux']
        col_type = configurations['col_type']
        df.insert(df.shape[1], col_name, 0)
        content = aux_source[col_aux].astype(col_type).unique().tolist()
        df.loc[:, col_name] = df.apply(
            lambda x: 1 if str(x[col_base]) in content else 0, axis=1
        )
        return df


class AuxiliaryFunctions:
    """
    Class with support functions for the functions in sources_global
    """
    @staticmethod
    def add_date_end_colname(df: pd.DataFrame, cols_to_verify: list = None) -> pd.DataFrame:
        """
        Move date from the middle of the column name to the end of the column name
        :param df: base dataframe
        :param cols_to_verify: list cols
        :return:
        """
        try:
            logger.info('Started add_date_end_colname')
            # Verify input
            assert type(df) == pd.DataFrame, "The df parameter must be a DataFrame"

            # Check if cols_to_verify is not None
            # if it is default: verify all the column in the dataframe
            if cols_to_verify is not None:
                # Check type of cols_to_verify
                assert type(cols_to_verify) == list, "The cols_to_verify parameter must be a list."
                # Verify if the columns given are present in the dataframe
                missing_cols = [col for col in cols_to_verify if col not in df.columns.tolist()]
                assert len(missing_cols) == 0, \
                    "The cols {} are missing from the dataframe".format(', '.join(missing_cols))
            else:
                cols_to_verify = df.columns.tolist()

            # find dates
            # pattern: patterns with at least 4 digits
            # They were separated in 3 patterns in order to not have more under_scores proliferating in the col_names
            first_pattern = re.compile("[0-9]{4,}_")
            second_pattern = re.compile("_[0-9]{4,}")
            pattern = re.compile("[0-9]{4,}")

            # Substitute in cols_to_verify the pattern by '' and
            # then join in the end of the col_name all the patterns found separated by '_'
            changed_cols = ['{}_{}'.format(pattern.sub(r'', second_pattern.sub(r'', first_pattern.sub(r'', vcol))),
                                           '_'.join(re.findall(pattern, vcol)))
                            if len(re.findall(pattern, vcol)) != 0
                            else vcol
                            for vcol in cols_to_verify]

            # substitute the new col_names in the dataframe
            df.columns = [changed_cols[cols_to_verify.index(col)]
                          if col in cols_to_verify
                          else col
                          for col in df.columns.tolist()
                          ]
            return df
        except Exception as e:
            logger.error(e)
            raise e

    @classmethod
    def verify_tags_in_list(cls, list_values: list, list_to_verify: list, return_dict: bool = False):
        """
        Choose the values of a list based in a substitution tag present in the list given
        if the list name doesn't have a substitution tag devolve as is
        :return:
        """
        try:
            logger.info('Started verify_tags_in_list')

            # Verify input
            assert type(list_values) == list, "The list_values parameter must be a list"
            assert type(list_to_verify) == list, "The list_to_verify parameter must be a list"

            # Verify if the user give the return_dict
            assert type(return_dict) == bool, "The return_dict parameter must be a boolean"

            # Start building the result
            if return_dict:
                result = {}
            else:
                result = []
            logger.info(list_to_verify)
            for val_name in list_to_verify:
                # Verify if the col_name is a string
                assert type(val_name) == str, "The value parameter must be a string"
                # Verify if it has a tag
                possible_subs_tags = ['#all-cols-starts-with#', '#all-cols-ends-with#',
                                      "#all-cols-middle-with#", "#all-cols-contain#",
                                      '#all-cells-starts-with#', '#all-cells-ends-with#',
                                      "#all-cells-middle-with#", "#all-cells-contain#"
                                      ]
                verify_tag = [tag for tag in possible_subs_tags if tag in val_name]

                # If col_name don't have tag give to the result as is to result_cols
                if len(verify_tag) == 0:
                    result_values = [val_name]
                    verify_tag = ['']

                # If col_name have tag search for the respective columns in the dataframe to give in result_cols
                else:
                    # Apply the verified tags
                    # Remove tags from col_name
                    val_name_subs = re.sub(r'|'.join(map(re.escape, possible_subs_tags)), '', val_name)

                    # Start cycle to apply the tags
                    result_values = []
                    for tag in verify_tag:
                        if tag in ['#all-cells-starts-with#', '#all-cols-starts-with#']:
                            # if tag == '#all-cols-starts-with#' search for the columns that begin with val_name
                            vals_with_tag = [val for val in list_values if val.startswith(val_name_subs)]
                            result_values.extend(vals_with_tag)
                        elif tag in ['#all-cells-ends-with#', '#all-cols-ends-with#']:
                            # if tag == '#all-cols-ends-with#' search for the columns that finish with val_name
                            vals_with_tag = [val for val in list_values if val.endswith(val_name_subs)]
                            result_values.extend(vals_with_tag)

                        elif tag in ['#all-cells-middle-with#', '#all-cols-middle-with#']:
                            # if tag == '#all-cols-ends-with#' search for the columns that finish with val_name
                            vals_with_tag = [val for val in list_values
                                             if ((val_name_subs in val) and
                                                 (not val.endswith(val_name_subs)) and
                                                 (not val.startswith(val_name_subs)))]
                            result_values.extend(vals_with_tag)
                        else:
                            # if tag == '#all-cols-contain#' search for the columns that contains col_name in the name
                            vals_with_tag = [val for val in list_values if val_name_subs in val]
                            result_values.extend(vals_with_tag)

                if return_dict:
                    result[val_name] = {'tag': verify_tag,
                                        'values': result_values}
                else:
                    result.extend(result_values)
                    # Remove duplicates preserving the order
                    duplicated_results = set()
                    result = [x
                              for x in result
                              if not (x in duplicated_results or duplicated_results.add(x))]

            return result
        except Exception as e:
            logger.error(e)

    @classmethod
    def verify_and_choose_columns_by_tag(cls, df: pd.DataFrame, cols_names: list, return_dict: bool = False):
        """
        Choose columns based in a substitution tag present in the column name
        if the column name doesn't have a substitution tag devolve as is
        :param return_dict: if True its returned a dictionary with the tag information and the resulted columns
        :param df: base dataframe
        :param cols_names: column name to verify
        :return:
        """
        try:
            logger.info('Started verify_and_choose_columns_by_tag')

            # Verify input
            assert type(df) == pd.DataFrame, "The df parameter must be a DataFrame"
            assert type(cols_names) == list, "The col_names parameter must be a list"

            # Verify if the user give the return_dict
            assert type(return_dict) == bool, "The return_dict parameter must be a boolean"

            # Run verify_tags_in_list
            result = cls.verify_tags_in_list(df.columns.tolist(), cols_names, return_dict)

            if not return_dict:
                logger.info('Final cols after verify columns tag {}'.format(', '.join(result)))
            return result
        except Exception as e:
            logger.error(e)
