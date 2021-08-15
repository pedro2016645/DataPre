"""
Script
"""
import logging
import os
import pandas as pd

logger = logging.getLogger(__name__)


class SaveExcelFiles:
    """
    Class with functions to help saving files in excel
    """

    @classmethod
    def __highlight(cls, s: pd.Series) -> list:
        """
        Function to create strips with colors in excel
        :return:
        """
        colors = []
        for i in s.index.values:
            if i % 2 == 0:
                colors.append('background-color: #6497b1')
            else:
                colors.append('background-color: #b3cde0')
        return colors

    @classmethod
    def save(cls, path_to_save: str, file_name: str, df: pd.DataFrame, table_schema: dict,
             src_time: str, ) -> tuple:
        """
        Function to save Outbound_all_suppliers file
        :param path_to_save: path were the file will be saved
        :param file_name: name of the excel file
        :param df: table with information to save in the excel
        :param table_schema: dictionary with information about the way the excel will be saved
        :param src_time:
        :return: None
        """

        logger.info('Saving file: {}'.format(file_name))

        # Check parameters
        print(path_to_save)
        assert type(path_to_save) == str, "The path_to_save parameter must be a string."
        assert os.path.exists(path_to_save), "The given path must exist"
        assert type(file_name) == str, "The file_name parameter must be a string"
        assert type(df) == pd.DataFrame, "The df parameter must be a DataFrame"
        assert type(table_schema) == dict or table_schema is None, "The table_schema parameter must be a dictionary."
        assert type(src_time) == str, "The source_tag must be a string"

        if table_schema is not None:
            # Create writer
            file_name = '{0}_{1}.xlsx'.format(file_name, src_time)
            path_to_save = os.path.join(path_to_save, file_name)
            writer = pd.ExcelWriter(path_to_save, engine='xlsxwriter')
            # create workbook
            workbook = writer.book
            # format header
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': False,
                'valign': 'top',
                'fg_color': '#3191eb',
                'border': 1,
                'align': 'center',
                'color': '#ffffff'
            })
            # add sheets
            for k in table_schema.keys():
                properties = table_schema[k]
                df.to_excel(
                        writer,
                        sheet_name=k,
                        index=False,
                        header=bool(properties['header']),
                        engine='xlsxwriter')

                for col_num, value in enumerate(df.columns.values.tolist()):
                    writer.sheets[k].write(0, col_num, value, header_format)
                    writer.sheets[k].set_column(col_num, col_num, width=34)

            writer.save()
        else:
            file_name = '{0}_{1}.csv'.format(file_name, src_time)
            path_to_save = os.path.join(path_to_save, file_name)
            df.to_csv(path_to_save, index=False,
                      decimal=",",
                      encoding='ANSI',
                      sep=";")
        return file_name, path_to_save
