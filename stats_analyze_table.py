"""Build regressions and such for a given table."""
import argparse

from sklearn.decomposition import PCA
import pandas
import numpy

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Analyze table')
    parser.add_argument('table_path',  help='Path to data table')
    args = parser.parse_args()

    table_df = pandas.read_csv(args.table_path)
    table_df = table_df.select_dtypes(include=[numpy.number]).dropna()
    print(table_df)
    # You must normalize the data before applying the fit method
    print('normalize')
    df_normalized = (table_df - table_df.mean()) / (table_df.std()+1e-6)
    pca_table = df_normalized[['1_1_0_LST_Day_1km', '1_1_10_LST_Day_1km']]
    print(pca_table)
    print('calc pca')
    pca = PCA(n_components=pca_table.shape[1])
    pca.fit(df_normalized)

    # Reformat and view results
    loadings = pandas.DataFrame(
        pca.components_.T,
        columns=['PC%s' % _ for _ in range(len(df_normalized.columns))],
        index=table_df.columns)
    print(loadings)
