import pandas as pd
import logging

logger = logging.getLogger(__name__)

class Financial:

    @staticmethod
    def financial_autonomy(df: pd.DataFrame, **variables) -> pd.DataFrame:
        """
        This function returns the DataFrame with the column financial_autonomy:
        :param df: Receives the base DataFrame
        :return: DataFrame with the column financial autonomy
        """
        equity_capital = variables['equity_capital']
        total_financial_assets = variables['total_financial_assets']

        assert type(df) == pd.DataFrame, "The df parameter is not a DataFrame."
        assert type(equity_capital) == str, "The equity_capital parameter is not a string"
        assert type(total_financial_assets) == str, "The total_financial_assets parameter is not a string"

        not_in_info_cols = [x for x in [equity_capital, total_financial_assets] if x not in list(df.columns)]
        assert len(not_in_info_cols) == 0, 'The columns {0} are not present in df'.format(",".join(
            not_in_info_cols))

        df.loc[:, equity_capital] = df[equity_capital].astype(float)
        df.loc[:, total_financial_assets] = df[total_financial_assets].astype(float)

        df.insert(
            df.shape[1],
            'financial_autonomy',
            df[equity_capital] / df[total_financial_assets])

        return df

    @staticmethod
    def debt(df: pd.DataFrame, non_current_funding: str, current_funding: str, financial_liabilities_for_trading: str,
             other_financial_liabilities: str) -> pd.DataFrame:
        """
        This function returns the DataFrame with the column debt:
        :param df:
        :param non_current_funding:
        :param current_funding:
        :param financial_liabilities_for_trading:
        :param other_financial_liabilities:
        :return:
        """

        assert type(df) == pd.DataFrame, "The df parameter is not a DataFrame."
        assert type(non_current_funding) == str, "The equity_capital parameter is not a string"
        assert type(current_funding) == str, "The current_funding parameter is not a string"
        assert type(financial_liabilities_for_trading) == str, \
            "The financial_liabilities_for_trading parameter is not a string"
        assert type(other_financial_liabilities) == str, "The other_financial_liabilities parameter is not a string"

        cols_for_calculation = [non_current_funding,
                                current_funding,
                                financial_liabilities_for_trading,
                                other_financial_liabilities]

        not_in_info_cols = [x for x in cols_for_calculation if x not in list(df.columns)]
        assert len(not_in_info_cols) == 0, 'The columns {0} are not present in df'.format(",".join(
            not_in_info_cols))

        for col in cols_for_calculation:
            df.loc[:, col] = df[col].astype(float)

        df.insert(
            df.shape[1],
            'debt',
            df[non_current_funding] + df[current_funding] + df[financial_liabilities_for_trading] +
            df[other_financial_liabilities]
        )

        return df

    @staticmethod
    def netdebt(df: pd.DataFrame, debt: str, cash_and_bank_deposits: str) -> pd.DataFrame:
        """
        This function returns the DataFrame with the column netdebt:
        :param df:
        :param debt:
        :param cash_and_bank_deposits:
        :return:
        """

        assert type(df) == pd.DataFrame, "The df parameter is not a DataFrame."
        assert type(debt) == str, "The equity_capital parameter is not a string"
        assert type(cash_and_bank_deposits) == str, "The current_funding parameter is not a string"

        cols_for_calculation = [debt,
                                cash_and_bank_deposits]

        not_in_info_cols = [x for x in cols_for_calculation if x not in list(df.columns)]
        assert len(not_in_info_cols) == 0, 'The columns {0} are not present in df'.format(",".join(
            not_in_info_cols))

        for col in cols_for_calculation:
            df.loc[:, col] = df[col].astype(float)

        df.insert(
            df.shape[1],
            'netdebt',
            df[debt] - df[cash_and_bank_deposits]
        )

        return df

    @staticmethod
    def ebidta(df: pd.DataFrame, cols_to_consider: str) -> pd.DataFrame:
        """
        Function to determine ebitda
        :param df:
        :param cols_to_consider:
        :return:
        """
        try:
            logger.info("Adding ebidta")
            cols_to_consider = cols_to_consider.split(",")
            cols_not_present = [x for x in cols_to_consider if x not in list(df.columns)]
            assert len(cols_not_present) == 0, "The cols: {} are not present at the source".format(
                ", ".join(cols_not_present))

            df.insert(df.shape[0], 'ebidta', df[cols_to_consider].sum(1))

        except ValueError as e:
            logger.error(e)
            raise e

        return df


