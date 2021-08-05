"""Build regressions and such for a given table."""
import argparse

from sklearn import linear_model
from sklearn.metrics import r2_score
from sklearn.decomposition import PCA
import pandas
import numpy

import matplotlib.pyplot as plt


def main():
    parser = argparse.ArgumentParser(description='Analyze table')
    parser.add_argument('table_path',  help='Path to data table')
    parser.add_argument('predictor_path',  help='Path to predictor variables')
    parser.add_argument('response_path',  help='Path to response variables')
    parser.add_argument('--drop_missing', action='store_true')
    args = parser.parse_args()

    with open(args.response_path) as response_file:
        response_list = response_file.read().split('\n')[:-1]

    with open(args.predictor_path) as predictor_file:
        predictor_list = predictor_file.read().split('\n')[:-1]

    table_df = pandas.read_csv(args.table_path)
    table_df = table_df.select_dtypes(include=[numpy.number])

    if args.drop_missing:
        table_df = table_df.dropna()
    else:
        table_df.fillna(table_df.mean(), inplace=True)

    # uncomment below if you want it to guess the dimensionality
    pca = PCA(n_components='mle', svd_solver='full')
    #pca = PCA(n_components=18, svd_solver='full')
    pca.fit(table_df[response_list])
    with open('stats_pca.csv', 'w') as pca_table:
        pca_table.write(f",{','.join(response_list)}\n")
        for index, pca_row in enumerate(pca.components_):
            print(pca_row)
            pca_table.write(f"je ne sais quoi {index+1},{','.join([str(v) for v in pca_row])}\n")
    print(pca.components_.shape)
    print(pca.explained_variance_ratio_)
    print(pca.singular_values_)

    reg = linear_model.MultiTaskLassoCV(normalize=True, max_iter=5000, verbose=True, n_jobs=-1, tol=0.01)
    #reg = linear_model.LinearRegression(normalize=True)
    reg.fit(table_df[predictor_list], table_df[response_list])

    prediction = reg.predict(table_df[predictor_list])
    r2 = r2_score(table_df[response_list], prediction)

    print(f'overall R^2 = {r2}')
    print(reg.coef_.shape)

    csv_path = 'stats_results.csv'
    with open(csv_path, 'w') as stats_file:
        stats_file.write(f'response_id,r2,intercept,{",".join(predictor_list)}\n')
        for response_index, response_id in enumerate(response_list):
            plt.plot(table_df[response_id], reg.predict(table_df[predictor_list])[:, response_index], '.')
            r2 = r2_score(table_df[response_id], prediction[:, response_index])
            ymin, ymax = plt.ylim()
            plt.title(f'{response_id}, R^2={r2}')
            plt.axis('tight')
            plt.savefig(f'stats_{response_id}.png')
            plt.clf()
            stats_file.write(f'{response_id},{r2},{reg.intercept_[response_index]},{",".join([str(v) for v in reg.coef_[response_index]])}\n')





if __name__ == '__main__':
    main()
