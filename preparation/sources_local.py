"""
Script with class to apply local transformations to sources, ie, apply functions that are specific to the each source
"""

import pandas as pd
import logging
import numpy as np

from shyness.scripts.preparation.sources import Standardization

DATE_FORMAT_SOURCE = "%Y%m%d_%H%M%S"
logger = logging.getLogger(__name__)


class DmeRepository:
    """
    Module for dme_repository source transformations
    """

    @staticmethod
    def if_prod(df: pd.DataFrame) -> pd.DataFrame:
        """
        This function gives a DataFrame with the indicator if the client have production
        :param df: Receives a DataFrame
        :return: DataFrame with if_prod column
        """
        assert type(df) == pd.DataFrame, 'The df parameter must be a DataFrame'

        # Select columns beginning with 'prod' and without 'total' in the column name
        prod_col_to_sum = [x for x in df.columns.tolist() if (x.startswith('prod')) and ('total' not in x)]
        df = Standardization.floats(df, prod_col_to_sum)

        df.insert(df.shape[1], 'if_prod', df[prod_col_to_sum].sum(axis=1))
        df.loc[:, 'if_prod'] = df['if_prod'].apply(lambda x: 1 if x > 0 else 0)

        return df


class Dun:
    """
    Module with functions to prepare DUN
    """

    @staticmethod
    def dun_mother_in_cgd(df: pd.DataFrame, **configurations) -> pd.DataFrame:
        """
        This function returns the DataFrame with an indicator if the dun mother is in CGD or not
        :param df: Receives the base DataFrame
        :return: DataFrame with column: if_dun_mother_in_cgd
        """

        # Check configurations content
        logger.info("Checking if dun_mother is in CGD")
        allowed_keys = ['aux_source', 'aux_cols']
        verify_keys = [x for x in allowed_keys if x not in configurations.keys()]
        assert len(verify_keys) == 0, "The parameters: {} are missing".format(", ".join(verify_keys))
        cols_info_client = configurations['aux_cols'].split(",")
        aux_source_tag = configurations['aux_source']

        assert aux_source_tag in configurations.keys(), "The auxiliary source {} is missing".format_map(aux_source_tag)
        df_info_client = configurations[aux_source_tag]

        assert type(df) == pd.DataFrame, "The df parameter is not a DataFrame"
        assert type(df_info_client) == pd.DataFrame, "The df_info_client parameter is not a DataFrame."
        assert type(cols_info_client) == list, "The cols_info_client parameter must be a list."

        cols_needed_df = ['duns_number', 'numero_contribuinte', 'duns_mae']
        not_in_info_cols = [x for x in cols_needed_df if x not in list(df.columns)]
        assert len(not_in_info_cols) == 0, 'The columns {0} are not present in df'.format(",".join(not_in_info_cols))

        not_in_info_cols = [x for x in cols_info_client if x not in list(df_info_client.columns)]
        assert len(not_in_info_cols) == 0, 'The columns {0} are not present in df'.format(",".join(not_in_info_cols))

        df.loc[:] = Standardization.nifs(df, ['numero_contribuinte'])
        df_info_client.loc[:] = Standardization.nifs(df_info_client, [cols_info_client[0]])

        df.loc[:, 'duns_mae_with_id'] = df['duns_mae'].isin(df['duns_number'].values.tolist())

        df = df.merge(
            df[['numero_contribuinte', 'duns_number']].rename(columns={'numero_contribuinte': 'nif_duns_mae'}),
            left_on='duns_mae', right_on='duns_number', how='left')

        cond_in_cgd = df_info_client[cols_info_client[1]].isna()
        df_info_client = df_info_client[~cond_in_cgd]
        df['if_mother_in_cgd'] = df['nif_duns_mae'].apply(
            lambda x: 1 if x in df_info_client[cols_info_client[0]] else 0)
        df = df.drop(columns='duns_number_y')
        df.rename(columns={'duns_number_x': 'duns_number'}, inplace=True)
        return df


class Documents:
    """
    Class with static/class method for local transformations for documents from clf
    """

    @staticmethod
    def add_anticipation_indicator(df: pd.DataFrame, **configurations) -> pd.DataFrame:
        """
        This function add an indicator for anticipations
        To be a document anticipated
        the value anticipated must be above 0
        the anticipated date must be not empty
        :param df: Dataframe with documents
        :return: dataframe with a new indicator
        """
        try:

            # Check configurations keys
            logger.info("Start function: add_anticipation_indicator")
            assert 'value_col' in configurations.keys(), "The value_col is not present on configurations params"
            assert 'date_col' in configurations.keys(), "The date_col is not present on configurations params"

            value_col = configurations['value_col']
            date_col = configurations['date_col']

            assert type(df) == pd.DataFrame, "The df parameter is not a DataFrame"
            assert value_col in list(df.columns), " The given df doesn't have {} column".format(value_col)
            assert date_col in list(df.columns), "The given df doesn't have {} column".format(date_col)

            logger.info("Input variables OK!")
            df.loc[:, value_col] = df[value_col].astype(float)
            df.insert(df.shape[1], 'if_anticipated', 0)

            # In order to be anticipated the intervention must have have anticipated values above 0 and an anticipation
            # date

            df.loc[df[df[value_col] > 0].index, 'if_anticipated'] = 1
            return df

        except Exception as e:
            logger.error(e)
            raise e

    @staticmethod
    def add_profit_by_intervention(df: pd.DataFrame, **configurations) -> pd.DataFrame:
        """
        Function that gives profit by intervention
        :param df: Receives de base DataFrame of documents
        :param configurations: Receives columns names
        :return: DataFrame with profit by intervention column
        """
        logger.info("Start function: intervention_profit")

        assert type(df) == pd.DataFrame, "The df parameter is not a DataFrame"

        df_aux_tag = configurations['aux_source']
        df_aux = configurations[df_aux_tag]

        assert type(df_aux) == pd.DataFrame, "The df_aux parameter is not a DataFrame"

        col_spread = configurations['col_spread']
        col_euribor = configurations['col_euribor']
        col_commission = configurations['col_commission']
        col_base = configurations['col_base']
        col_aux = configurations['col_aux']
        col_anticipation_date = configurations['col_anticipation_date']
        col_anticipation_amount = configurations['col_anticipation_amount']
        col_payment_date = configurations['col_payment_date']

        cols_needed_df = [col_base, col_anticipation_amount, col_anticipation_date, col_payment_date]
        cols_not_present = [x for x in cols_needed_df if x not in list(df.columns)]
        assert len(cols_not_present) == 0, 'The columns {0} are not present in df'.format(",".join(cols_not_present))

        cols_needed_df_aux = [col_aux, col_commission, col_spread, col_euribor]
        cols_not_present = [x for x in cols_needed_df_aux if x not in list(df_aux.columns)]
        assert len(cols_not_present) == 0, 'The columns {0} are not present in df'.format(",".join(cols_not_present))

        if 'col_type' in configurations.keys():
            df = df.astype({col_base: configurations['col_type']})
            df_aux = df_aux.astype({col_aux: configurations['col_type']})

        # Add spread and commission
        df = pd.merge(
            df,
            df_aux[[col_aux, col_spread, col_euribor, col_commission]],
            left_on=col_base,
            right_on=col_aux,
            how='left'
        )

        # convert date columns to datetime
        df.loc[:, col_payment_date] = pd.to_datetime(df[col_payment_date], format='%Y%m%d_%H%M%S', errors='coerce')
        df.loc[:, col_anticipation_date] = pd.to_datetime(df[col_anticipation_date],
                                                          format='%Y%m%d_%H%M%S',
                                                          errors='coerce')

        # Subtract anticipation date to payment date
        df['diff_payment_anticipation_dates'] = df[col_payment_date] - df[col_anticipation_date]
        df['diff_payment_anticipation_dates'] = df['diff_payment_anticipation_dates'] / np.timedelta64(1, 'D')
        df.loc[:, 'diff_payment_anticipation_dates'] = df['diff_payment_anticipation_dates'].fillna(0)
        # convert date columns to string
        df.loc[:, col_payment_date] = df[col_payment_date].dt.strftime('%Y%m%d_%H%M%S')
        df.loc[:, col_anticipation_date] = df[col_anticipation_date].dt.strftime('%Y%m%d_%H%M%S')
        # convert columns to float
        df.loc[:, [col_commission, col_spread, col_euribor, col_anticipation_amount]] = \
            df[[col_commission, col_spread, col_euribor, col_anticipation_amount]].fillna(0)
        for col in [col_commission, col_spread, col_euribor, col_anticipation_amount]:
            df.loc[:, col] = df[col].astype('float64')

        # Profit without commission
        df['profit_without_commission'] = df.apply(
            lambda x:
            x[col_anticipation_amount]*(x['diff_payment_anticipation_dates'] * (
                    x[col_euribor]/100 + x[col_spread]/100)/360),
            axis=1
        )

        # Total profit
        df['total_profit'] = df.apply(
            lambda x:
            x['profit_without_commission'] + x[col_anticipation_amount]*(x[col_commission]/100),
            axis=1
        )

        df.loc[:, 'total_profit'] = df['total_profit'].apply(lambda x: 0 if x < 0 else x)
        df.loc[:, 'profit_without_commission'] = df['profit_without_commission'].apply(lambda x: 0 if x < 0 else x)

        return df


class InternationalTransfers:
    """
    Module for the international transfers source transformations
    """

    @staticmethod
    def agg_types_transfers(df: pd.DataFrame, **configurations) -> pd.DataFrame:
        """
        This function gives a DataFrame with the issued and received international transfers of a specific type (sepa
        or non-sepa) for each n_cliente, represented with indicator columns. The original DataFrames contain metrics
        related with the amounts of international transfers without specifying whether these transfers were issued or
        received
        :param df: Receives a DataFrame
        :return: DataFrame with 1 new indicator column, related to the type of international transfer
        """
        assert type(df) == pd.DataFrame, 'The df parameter must be a DataFrame'

        df_aux_tag = configurations['aux_source']
        df_aux = configurations[df_aux_tag]

        assert type(df_aux) == pd.DataFrame, "The df_aux parameter is not a DataFrame"

        # Create indicator column that specifies whether the non-sepa international transfers were issued or received
        df.insert(df.shape[1]-2, "indicador_trf_emitidas_recebidas", 'emitidas')
        df_aux.insert(df_aux.shape[1]-2, "indicador_trf_emitidas_recebidas", 'recebidas')

        # Concatenate the dataframes in order to obtain a single dataframe with the respective indicator column
        final_df = pd.concat([df, df_aux])

        return final_df
